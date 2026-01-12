import os
import time

import math

from esperanza.benchbase.benchmark_session_cfp import BenchmarkSession
from esperanza.utils.download_mysql import download_mysql
from esperanza.utils.state_change_report import read_state_change_report

host = 'A'

DB_STATE_CHANGE_BASE_OPTIONS = [
    '-b', 'dbdump.sql',
    '-i', 'offchaindb',
    '-d', 'offchaindb',
    '-k', f'{host}_cfpval.partid, {host}_parts_tree.partid, {host}_parts_tree.parents_partid, {host}_assembler.partid'
]

DB_TABLE_DIFF_OPTIONS = {
    f'{host}_cfpval': ['partid', 'cfp', 'co2'],
    f'{host}_parts_tree': ['partid', 'parents_partid', 'qty'],
    f'{host}_assembler': ['partid', 'assembler']
}

def perform_state_change(session: BenchmarkSession, rollback_gids: list[int], do_extra_replay_st: bool=False, do_table_diff: bool=False):
    """
    db_state_changeを実行します.
    :param session: session オブジェクト
    :param rollback_gids: ロールバックするgid
    :param do_extra_replay_st: single-threadでフルリプレイを行うかどうか
    :param do_table_diff: single-threadで実行したリプレイとテーブル比較をするかどうか
    """

    logger = session.logger

    rollback_log_name = f"rollback_{rollback_gids[0]}_{rollback_gids[-1]}"
    rollback_plan_name = rollback_log_name + ".plan"

    with open(f"{session.session_path}/{rollback_plan_name}", "w") as f:
        f.write((",".join(map(lambda x: str(x), rollback_gids))))

    rollback_stdout_name = rollback_log_name + ".stdout"
    rollback_stderr_name = rollback_log_name + ".stderr"
    rollback_report_name = rollback_log_name + ".report.json"

    replay_log_name = rollback_log_name + ".replay"
    replay_stdout_name = replay_log_name + ".stdout"
    replay_stderr_name = replay_log_name + ".stderr"
    replay_report_name = replay_log_name + ".report.json"

    # # PREPARE 실행한다.
    session.run_db_state_change(
        DB_STATE_CHANGE_BASE_OPTIONS + [
            '-r', rollback_report_name,
            "rollback=-"
        ],
        pipe_stdin_file=f"{session.session_path}/{rollback_plan_name}",
        stdout_name=rollback_stdout_name,
        stderr_name=rollback_stderr_name
    )

    rollback_report = read_state_change_report(f"{session.session_path}/{rollback_report_name}")

    # REPLAY 실행한다.
    session.run_db_state_change(
        DB_STATE_CHANGE_BASE_OPTIONS + [
            '-N',
            '-r', replay_report_name,
            'replay'
        ],
        stdout_name=replay_stdout_name,
        stderr_name=replay_stderr_name,
        pipe_stdin_file=rollback_stdout_name
    )

    replay_report = read_state_change_report(f"{session.session_path}/{replay_report_name}")

    if do_extra_replay_st:
        replay_st_log_name = rollback_log_name + ".replay-st"
        replay_st_stdout_name = replay_st_log_name + ".stdout"
        replay_st_stderr_name = replay_st_log_name + ".stderr"
        replay_st_report_name = replay_st_log_name + ".report.json"
        # 위와 같은 조건으로 REPLAY를 한번 더 실행한다. (single-thread)
        session.run_db_state_change(
            DB_STATE_CHANGE_BASE_OPTIONS + [
                '-N',
                '-r', replay_st_report_name,
                "rollback=-:full-replay"
            ],
            pipe_stdin_file=f"{session.session_path}/{rollback_plan_name}",
            stdout_name=replay_st_stdout_name,
            stderr_name=replay_st_stderr_name,
        )

        replay_st_report = read_state_change_report(f"{session.session_path}/{replay_st_report_name}")

        session.mysqld.mysqldump(replay_st_report['intermediateDBName'], f"{session.session_path}/dbdump_st_latest.sql")
    else:
        replay_st_report = None

    logger.info(f"State Change Report: {rollback_log_name}")
    if do_extra_replay_st:
        logger.info(f"prepare: {rollback_report['executionTime']}, replay: {replay_report['executionTime']}, replay-st: {replay_st_report['executionTime']}")
    else:
        logger.info(f"prepare: {rollback_report['executionTime']}, replay: {replay_report['executionTime']}")

    if do_extra_replay_st and do_table_diff:
        # 테이블 비교를 한다.
        tmp_db_name = f"{replay_report['intermediateDBName']}_tmp"

        session.load_dump(tmp_db_name, f"{session.session_path}/dbdump_st_latest.sql")

        replace_query = rollback_report['replaceQuery'] \
            .replace('__INTERMEDIATE_DB__', replay_report['intermediateDBName']) \
            .replace('benchbase', tmp_db_name)

        session.eval(replace_query)

        for (table, cols) in DB_TABLE_DIFF_OPTIONS.items():
            session.tablediff(
                f"{tmp_db_name}.{table}",
                f"{replay_st_report['intermediateDBName']}.{table}",
                cols
            )


if __name__ == "__main__":

    if not download_mysql():
        print("MySQL distribution is not available")
        exit(1)

    host = 'A'

    DB_STATE_CHANGE_BASE_OPTIONS = [
        '-b', 'dbdump.sql',
        '-i', 'offchaindb',
        '-d', 'offchaindb',
        #'-k', f'{host}_cfpval.partid, {host}_parts_tree.partid, {host}_parts_tree.parents_partid, {host}_assembler.partid'
        '-k', f'{host}_cfpval.partid'
    ]

    DB_TABLE_DIFF_OPTIONS = {
        f'{host}_cfpval': ['partid', 'cfp', 'co2'],
        f'{host}_parts_tree': ['partid', 'parents_partid', 'qty'],
        f'{host}_assembler': ['partid', 'assembler']
    }

    os.putenv("DB_HOST", "localhost")
    os.putenv("DB_PORT", "3306")
    os.putenv("DB_USER", "deploy_user")
    os.putenv("DB_PASS", "password")

    session = BenchmarkSession("cfp", "1m")
    logger = session.logger
    #session.prepare()

    # statelogdを実行してbinary logからstatelogを生成します。
    session.run_statelogd(['-k', 'A_cfpval.partid'])

    # db_state_changeを実行するためにmysqldを起動します。
    logger.info("starting mysqld...")
    session.mysqld.start()

    # mysqldの起動が完了するまで待つ
    time.sleep(10)

    # クラスタの作成
    logger.info("creating cluster...")
    session.run_db_state_change(DB_STATE_CHANGE_BASE_OPTIONS + ['make_cluster'])

    # Gidに基づくロールバック処理
    perform_state_change(session, [100], do_extra_replay_st=False, do_table_diff=False)


    logger.info("stopping mysqld...")
    session.mysqld.stop()
