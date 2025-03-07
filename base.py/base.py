import json
import yaml
import boto3
from LayerRDS import RdsCustom
from LayerLogger import LoggerCustom
from datetime import datetime,timedelta
import pandas as pd

debugmode = False
logger = None
summarized_error_log_group_name = "summarized_error_log_mainte"
s3_client = boto3.client('s3')
# class FileCopyException(Exception):
#     def __init__(self, message):
#         self.message = message
#         super().__init__(self.message)

def lambda_handler(event, context):
    ### 開始共通処理：ここから ###
    global debugmode, logger  # 他関数でも参照できるようにスコープはグローバル
    if 'debugmode' in event:
        debugmode = event['debugmode']
        # _bucket_name = event['bucket_name']

    logger = LoggerCustom(context, "/aws/lambda/" + summarized_error_log_group_name, debugmode)
    logger.start_message()
    ### 開始共通処理：ここまで ###

    try:
        ##############################
        ## 初期設定
        ##############################
        # 変数初期化
        rds_layer = RdsCustom()
        sqlid_yaml_filename = "sqlids.yaml"

        # イベント変数で初期化
        ENV = event['ENV']
        enviroment = event['enviroment']
        _bucket_name = event['bucket_name']
        _rds_secrets_name = event['rds_secrets_name']
        rds_service_name = event['rds_service_name']
        s3_mode = False if 's3_mode' not in event else event['s3_mode']

        #ExecutionNameを変数に格納
        execution_name = event['context']['stepfunctions']['Execution']['Name']

        # 環境識別子を補完
        bucket_name = f"{enviroment}-{ENV}-{_bucket_name}"
        rds_secrets_name = f"{enviroment}-{ENV}-{_rds_secrets_name}"
        logger.debug(f"補完されたバケット名：{bucket_name}")

        # sqlalchemyエンジンの作成
        logger.info(f"シークレットキー {rds_secrets_name} でDB接続エンジン作成")
        engine = rds_layer.create_engine_secrets(rds_secrets_name, rds_service_name, echo=False)

        # SQL取得
        logger.info(f"sqlidとSQLファイルの設定ファイル読み込み {sqlid_yaml_filename}")
        with open(sqlid_yaml_filename) as file:
            dict_sqlids = yaml.safe_load(file)

        logger.info(f"SQLファイル取得と構造文部分の変数展開")
        if s3_mode:
            logger.info(f"SQLファイル取得元をS3に設定")
            rds_layer.set_s3_mode(bucket_name)

        dict_sqlstatement = dict()
        dict_sqlstatement = rds_layer.get_sql_statement(dict_sqlids['sqlids'])
        if debugmode:
            logger.debug(f"dict_sqlstatement:{dict_sqlstatement}")

        # gnkkisn_prmt_rnki_idをカンマ区切り文字列に変換
        gnkkisn_prmt_rnki_id_list = event['gnkkisn_prmt_rnki_id']
        gnkkisn_prmt_rnki_id_str = ', '.join(f"'{id}'" for id in gnkkisn_prmt_rnki_id_list)

        
        #####################################################
        ## 個別処理
        #####################################################
        base_backet = "ProdMgmt/CostAccounting/Current/Param/"
        #base_backetの存在確認
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_backet, Delimiter='/')
        moved_files = []  # 初期化
        file_path = ""
        with engine.connect() as conn:
            response, tmp_bucket_path = get_temp_folder(bucket_name, context,execution_name,base_backet)
            #########################################################################
            ##処理フロー
            #########################################################################
            # 3.制御情報取得
            #####################################
            # ループ開始
            #####################################
            # 4.ファイル出力可否チェック
            # ファイル出力可なら次の処理へ

            # 5.ファイル出力状況ステータス更新
            # 6.出力情報追加
            #####################################
            # ループ終了
            #####################################
            # 出力情報の件数を確認し、0件ならそのまま終了
            # 7.ファイル出力
            # 8.ファイル出力状況ステータス更新(出力済み)
            # 9.ファイル出力状況ステータス更新(エラー)
            # 10.終了処理
    except Exception as e:
        logger.error(e)
        raise Exception(e)

##############################
## 一時フォルダ内のファイルの存在確認
##############################
def check_file_exists(bucket_name, base_backet, execution_name):
    check_bucket = f"{base_backet}Temp/{execution_name}/"
    logger.debug(check_bucket)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=check_bucket)
    return response

##############################
## 3.パラメータ一時フォルダ取得
##############################
def get_temp_folder(bucket_name, context,execution_name,base_backet):
    # 3-1.contextからExceptionNameを取得
    # 後でtmp_bucket_nameの内容をcontext.ExceptionNameに変更する
    #[探すバケット名]
    # tmp_bucket = "sample"
    tmp_bucket  = execution_name
    
    # [探す場所]
    # tmp_bucket_path = "work/numazaki/Temp/"
    tmp_bucket_path = f"{base_backet}Temp/{execution_name}/"
    logger.info(f"tmp_bucket_path:{tmp_bucket_path}")
    # 検索処理
    response = s3_client.list_objects_v2(
    Bucket=bucket_name,
    Prefix=tmp_bucket_path,
    Delimiter="/"
    )
    return response,tmp_bucket_path
##############################
## 4.トリガーファイルID設定
##############################
def set_trigger_file_id(bucket_name,context):
    # トリガーIDを生成
    jst_offset = timedelta(hours=9)
    now_jst = datetime.now() + jst_offset
    #トリガーIDの桁数修正が入るまではこっちで運用(変更対応後はコメントアウト)
    trigger_id = now_jst.strftime('%Y%m%d%H%M')
    trigger_id += now_jst.strftime('%S')[0]
    #トリガーIDの桁数修正が入ったらこっちで運用
    # trigger_id = now_jst.strftime('%Y%m%d%H%M%S') + now_jst.strftime('%f')[:3] 
    logger.debug(f"作成されたトリガーID:{trigger_id}")

    # フォルダパスの変数化
    folder_path = f"ProdMgmt/CostAccounting/Current/Param/{trigger_id}/"
    # S3にフォルダを作成するため、空のオブジェクトをアップロード
    s3_client.put_object(Bucket=bucket_name, Key=folder_path)
    logger.debug(f"フォルダ作成完了{folder_path}")

    return trigger_id, folder_path
##############################
## 5.一時フォルダからトリガーIDフォルダへファイルコピー
##############################
def copy_files_to_trigger_folder(bucket_name, trigger_bucket,base_backet,execution_name):
    tmp_bucket = f"{base_backet}Temp/{execution_name}/"
    logger.debug(tmp_bucket)
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=tmp_bucket,
        Delimiter="/"
    )
    before_mv_file = len(response['Contents']) - 1
    logger.info(f"コピー元フォルダのファイル数:{before_mv_file}")
    moved_files = []  # コピーしたファイル名を保存するリスト

    for obj in response['Contents']:
        file_key = obj['Key']  # オブジェクトキー（ファイルのパス）
        file_name = file_key.split('/')[-1]
        # コピー先のキーを生成
        new_key = f"{trigger_bucket}{file_name}"
        # 実際のコピー処理
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': file_key},
            Key=new_key
        )
        # コピーしたファイル名をリストに追加
        moved_files.append(file_name)


    ##############################################################
    # ここでエラーパターン検証用にコピー後のファイルから意図的にコピー前フォルダに１ファイルだけ戻す処理を入れる
    # この処理は本番環境では削除する
    ##############################################################
    # # ここでコピーしたファイルの1つを元のフォルダに戻す
    # if moved_files:
    #     file_to_return = moved_files.pop(0)  # コピーしたファイルリストの最初のファイルを選択
    #     source_key = f"{trigger_bucket}{file_to_return}"
    #     destination_key = f"{base_backet}{file_to_return}"
    #     # 戻したファイルをコピー先から削除
    #     s3_client.delete_object(Bucket=bucket_name, Key=source_key)
    ##############################################################

    # コピー後のファイル数確認
    after_response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=trigger_bucket,
        Delimiter="/"
    )
    

    after_mv_file = len(after_response['Contents']) - 1
    logger.info(f"コピー先フォルダのファイル数:{after_mv_file}")
    

    # コピーしたファイル名のリストを返す
    return before_mv_file, after_mv_file, moved_files


##############################
## 6.連携状況ステータス更新(エラー)
##############################
def update_status_on_failure(conn, rds_layer, dict_sqlstatement, dict_sqlids, gnkkisn_prmt_rnki_id_str):
    target_sqlid = 'trigger_file_out1'
    target_sqlstr = dict_sqlstatement[target_sqlid]

    # IN句を直接埋め込む
    target_sqlstr = target_sqlstr.replace(":gnkkisn_prmt_rnki_id", gnkkisn_prmt_rnki_id_str)

    # バインド変数
    bind_params = {
        'rnkijky_stat_cd': '9'
    }

    # SQL実行
    result = rds_layer.execute_modifydata(
        conn, target_sqlstr, bind_params, check_mode='NO_CHECK'
    )
    return result


##############################
## 6.2　トリガーIDフォルダの削除
##############################
def delete_trigger_id(bucket_name, trigger_bucket):
    # トリガーIDフォルダ内のすべてのオブジェクトをリストアップ
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=trigger_bucket)
    if 'Contents' in response:
        # フォルダ内のオブジェクトを削除
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        # オブジェクトを一括削除
        s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': objects_to_delete}
        )
        logger.error(f"トリガーIDフォルダ内のすべてのファイルを削除しました: {trigger_bucket}")
    else:
        logger.error(f"トリガーIDフォルダに削除対象のファイルが存在しません: {trigger_bucket}")

    # フォルダ自体を削除（空フォルダの削除）
    s3_client.delete_object(Bucket=bucket_name, Key=trigger_bucket)
    logger.error(f"トリガーIDフォルダを削除しました: {trigger_bucket}")

##############################
## 7-1.一時フォルダを削除
##############################
def delete_work_folder(bucket_name,base_backet,execution_name):
    tpm_folder = f"{base_backet}Temp/{execution_name}/"
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=tpm_folder,
        Delimiter="/"
    )
    if 'Contents' in response:
        # フォルダ内のオブジェクトを削除
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        # オブジェクトを一括削除
        s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': objects_to_delete}
        )
    logger.info(f"一時フォルダ削除完了: {tpm_folder}")

##############################
## 7-1.再計算チェック
##############################
def check_recalculation(conn, rds_layer, dict_sqlstatement, dict_sqlids, gnkkisn_prmt_rnki_id_str):
    target_sqlid = 'trigger_file_out2'
    target_sqlstr = dict_sqlstatement[target_sqlid]

    # IN句を直接埋め込む
    target_sqlstr = target_sqlstr.replace(":gnkkisn_prmt_rnki_id", gnkkisn_prmt_rnki_id_str)

    # SQL実行
    result = rds_layer.execute_select(
        conn, target_sqlstr, {}, check_mode='>=ZERO'
    )

    # 結果をリスト形式に変換
    list_result = [
        {'gnkkisn_prmt_rnki_id': row[0], 'cost_set': row[1]}
        for row in result['rows']
    ]
    logger.debug(f"再計算対象データ: {list_result}")
    return list_result
##############################
## 8.トリガーファイル配置
##############################
def place_trigger_file(list, bucket_name, trigger_bucket, trigger_id):
    import csv
    import io
    calc_flag = ""
    base_bucket = "ProdMgmt/CostAccounting/Current/Param/"

    # フラグの判定
    if len(list) == 0:
        calc_flag = "通常"
    else:
        calc_flag = "再計算"

    # CSV ファイル名の作成
    file_name = f"IF_IBSR001_CH_GS_00_{trigger_id}.csv"
    file_path = base_bucket + file_name

    recalc_file_name = f"IF_IBDI001_CH_GS_00_{trigger_id}.csv"
    recalc_file_path = base_bucket + recalc_file_name

    # CSV ファイルの内容を準備
    csv_buffer = io.StringIO()

    if calc_flag == "再計算":
        # フラグが"通常"の場合、trigger_bucket をそのまま出力
        csv_buffer.write(f"{trigger_bucket}\n")
        # S3 に CSV をアップロード
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )
        # アップロード完了ログ
        logger.debug(f"CSVファイル配置完了:{file_path}")
    else:
        # フラグが"再計算"の場合、list 内の cost_set を改行区切りで出力
        for item in list:
            csv_buffer.write(f"{item.get('cost_set', '')}\n")
            s3_client.put_object(
            Bucket=bucket_name,
            Key=recalc_file_path,
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )
        # アップロード完了ログ
        logger.debug(f"CSVファイル配置完了:{recalc_file_path}")


        
    return file_path

##############################
## 9.連携状況ステータス更新(連携済み)
##############################
def update_status_on_success(conn, rds_layer, dict_sqlstatement, dict_sqlids, gnkkisn_prmt_rnki_id_str, trigger_id):
    target_sqlid = 'trigger_file_out3'
    target_sqlstr = dict_sqlstatement[target_sqlid]

    # IN句を直接埋め込む
    target_sqlstr = target_sqlstr.replace(":gnkkisn_prmt_rnki_id", gnkkisn_prmt_rnki_id_str)

    # バインド変数
    bind_params = {
        'trg_id': trigger_id,
        'rnki_dt': datetime.now() + timedelta(hours=9),  # JST時間
        'rnkijky_stat_cd':'2'
    }

    # SQL実行
    result = rds_layer.execute_modifydata(
        conn, target_sqlstr, bind_params, check_mode='NO_CHECK'
    )
    return result

##############################
## 10.終了処理
##############################
def end_process(file_path, moved_files):
    if len(moved_files) == 0:
        logger.error("フォルダなしのため終了")
    else:
        # トリガーファイル出力成功のメッセージ
        logger.info("トリガーファイル出力成功：")
        logger.info(f"出力成功したトリガーファイル名：{file_path}")
        # コピーしたファイル名を1つずつ出力
        for file_name in moved_files:
            logger.info(f"出力成功したコピーしたファイル名：{file_name}")
    return