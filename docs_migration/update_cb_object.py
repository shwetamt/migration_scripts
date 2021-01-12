
import grpc
from couchbase.bucket import Bucket
import os
from tickleDb.document_pb2_grpc import DocServiceStub
from docs_data_read_from_cb import get_failed_cb_reads_companies

TICKLE_DB_URL = 'tickledbdocservice.internal-grpc.titos.mindtickle.com:80'
channel = grpc.insecure_channel(TICKLE_DB_URL)
channel_ready_future = grpc.channel_ready_future(channel)
channel_ready_future.result(timeout=10)
stub = DocServiceStub(channel)

cbhost = '10.11.120.220:8091'

cb = Bucket('couchbase://' + cbhost + '/ce', username='mindtickle', password='testcb6mindtickle')

sub_dir = ''

def get_dir(prefix, sub_dir):
    current_dir = os.getcwd()
    dir = os.path.join(sub_dir, f'{prefix}_companies_media')
    if prefix == '':
        dir = sub_dir
    path = os.path.join(current_dir, dir)
    if not os.path.exists(path):
        os.makedirs(path)
    return path




def update_cb_object(comp_id):
    import couchbase_core.subdocument as SD

    medias = ["DOCUMENT_WORD","DOCUMENT_POWERPOINT","DOCUMENT_EXCEL","DOCUMENT_PDF","AUDIO", "VIDEO"]
    doc_id = f'{comp_id}.settings'
    try:
        r1=cb.get(doc_id)
        value = r1.value
        mig = value.get('extra_config', {}).get('picassoMigratedMedia','')
        if len(mig)>0:
            mig=mig.split(',')
        else:
            mig=[]
        new_mig = ",".join(list(set(mig + medias)))
        value.setdefault('extra_config',{})['picassoMigratedMedia'] = new_mig
        cb.upsert(doc_id, value)
        # cb.mutate_in(doc_id, SD.upsert("picassoMigratedMedia.test", "newValue", create_parents=True))
        # resp = N1QLRequest(
        # _N1QLQuery('UPDATE ce SET extra_config.picassoMigratedMedia = $1 WHERE META().id=$2', medias, doc_id ), cb)
    except Exception as e:
        print(f'Could not modify cb object - exception -{e}')



def enable_picasso(comp_list, sub_dir):
    failed_reads = get_failed_cb_reads_companies()
    failed_infra = get_dir("failed_infra", sub_dir)
    migrated_infra = get_dir("migrated_infra", sub_dir)
    failed_mapping = get_dir("failed_picasso_mapping", sub_dir)
    successful_mapping = get_dir("successful_picasso_mapping", sub_dir)
    for comp_id in comp_list:
        cond1 = os.path.exists(f'{migrated_infra}/migrated_infra_{comp_id}.csv') and not os.path.exists(f'{failed_infra}/failed_infra_{comp_id}.csv')
        cond2 = os.path.exists(f'{successful_mapping}/successful_picasso_mapping_{comp_id}.csv') and not os.path.exists(
            f'{failed_mapping}/failed_picasso_mapping_{comp_id}.csv')
        if comp_id not in failed_reads and cond1 and cond2:
            update_cb_object(comp_id)


def enable_flag(comp_id):
    update_cb_object(comp_id)

if __name__=='__main__':
    id = '1268440569138087604'
    update_cb_object(id)
