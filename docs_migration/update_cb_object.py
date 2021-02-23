
import grpc
from couchbase.bucket import Bucket
import os
import csv
from tickleDb.document_pb2_grpc import DocServiceStub
from docs_data_read_from_cb import get_failed_cb_reads_companies

# TICKLE_DB_URL = 'tickledbdocservice.internal-grpc.prod.mindtickle.com:80'
# channel = grpc.insecure_channel(TICKLE_DB_URL)
# channel_ready_future = grpc.channel_ready_future(channel)
# channel_ready_future.result(timeout=10)
# stub = DocServiceStub(channel)

# cbhost = '10.11.120.220:8091'
# cb = Bucket('couchbase://' + cbhost + '/ce', username='mindtickle', password='testcb6mindtickle')

# cbhost = 'cb6-node-1-staging.mindtickle.com:8091'
# cbhost = 'cb-backup-ce-node-1.internal.mindtickle.com:8091'
cbhost = 'cb-6-node-1.internal.mindtickle.com:8091'
# cb = Bucket('couchbase://' + cbhost + '/ce', username='couchbase', password='couchbase')
cb = Bucket('couchbase://'+cbhost+'/ce', username='mindtickle', password='d36b98ef7c6696eda2a6ber3')
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


flags = {0: 'picassoMedia', 1: 'picassoMigratedMedia'}
rev_flags = {'picassoMedia': 0, 'picassoMigratedMedia': 1}


def get_already_updated_objects():
    already_updated = set()
    path = get_dir("", sub_dir)
    if not os.path.exists(f'{path}/updated_cb_objects.csv'):
        return set()
    with open(f'{path}/updated_cb_objects.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            already_updated.add((row[0], rev_flags[row[1]]))
    return already_updated

def update_cb_object(comp_id, obj_writer, flag):
    import couchbase_core.subdocument as SD
    pic_medias = ["DOCUMENT_WORD","DOCUMENT_POWERPOINT","DOCUMENT_EXCEL","DOCUMENT_PDF","AUDIO", "VIDEO"]
    medias = ["DOCUMENT_WORD","DOCUMENT_PPT","DOCUMENT_XLS","DOCUMENT_PDF","AUDIO", "VIDEO"]
    doc_id = f'{comp_id}.settings'
    try:
        r1=cb.get(doc_id)
        value = r1.value
        # if value.get('extra_config') is None:
        #     value['extra_config'] = {}
        # migMedia = value['extra_config'].get('picassoMigratedMedia','')
        # picMedia = value['extra_config'].get('picassoMedia', '')
        # if len(migMedia)>0:
        #     migMedia=migMedia.split(',')
        # else:
        #     migMedia=[]
        # if len(picMedia)>0:
        #     picMedia=picMedia.split(',')
        # else:
        #     picMedia=[]
        # new_migMedia = ",".join(list(set(migMedia + medias)))
        # new_picMedia = ",".join(list(set(picMedia + picMedia)))
        if value.get('extra_config') is None:
            value['extra_config'] = {}
        if flag==0:
            # value['extra_config']['picassoMedia'] = "VIDEO"
            value['extra_config']['picassoMedia'] = ",".join(pic_medias)
        elif flag==1:
            # value['extra_config']['picassoMigratedMedia'] = "VIDEO"
            value['extra_config']['picassoMigratedMedia'] = ",".join(medias)
        else:
            value['extra_config']['picassoMedia'] = ",".join(pic_medias)
            value['extra_config']['picassoMigratedMedia'] = ",".join(medias)
            # value['extra_config']['picassoMedia'] = "VIDEO"
            # value['extra_config']['picassoMigratedMedia'] = "VIDEO"

        # value['extra_config']['picassoMedia'] = new_mig
        # value['extra_config']['reflowEnabled'] = True
        cb.upsert(doc_id, value)
        # obj_writer.writerow([comp_id, flags[flag]])
        print(f'success - {comp_id} - {flag}')
        # cb.mutate_in(doc_id, SD.upsert("picassoMigratedMedia.test", "newValue", create_parents=True))
        # resp = N1QLRequest(
        # _N1QLQuery('UPDATE ce SET extra_config.picassoMigratedMedia = $1 WHERE META().id=$2', medias, doc_id ), cb)
    except Exception as e:
        print(f'Could not modify cb object - exception -{e}')



def enable_picasso(comp_list, dir, flag):
    print(f'updating picasso flags...')
    global sub_dir
    sub_dir = dir
    already_updated_obj = get_already_updated_objects()
    path =get_dir('', sub_dir)
    fl = open(f'{path}/updated_cb_objects.csv', 'a')
    obj_writer = csv.writer(fl)
    for comp_id in comp_list:
        # update_cb_object(comp_id, obj_writer, flag)
        if (comp_id, flag) not in already_updated_obj:
            update_cb_object(comp_id, obj_writer, flag)
    fl.close()
    print(f'updated picasso flags')



# def enable_flag(comp_id):
#     update_cb_object(comp_id)
#
# if __name__=='__main__':
#     # id = '1325703240109091977'
#     id = '1261175967008605974'
#     update_cb_object(id)
