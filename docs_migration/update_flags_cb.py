
from couchbase.bucket import Bucket
from couchbase_core.n1ql import _N1QLQuery, N1QLRequest

cbhost = 'cb6-node-1-staging.mindtickle.com:8091'

cbhost_read = 'cb-backup-ce-node-1.internal.mindtickle.com:8091'
cbhost_write = 'cb-6-node-1.internal.mindtickle.com:8091'
# ce-cb6-backup
cb_read = Bucket('couchbase://'+cbhost_read+'/ce', username='couchbase', password='couchbase')

cb_write = Bucket('couchbase://'+cbhost_write+'/ce', username='mindtickle', password='d36b98ef7c6696eda2a6ber3')

# cbhost = 'cb-backup-ce-node-1.internal.mindtickle.com:8091'

cb = Bucket('couchbase://' + cbhost + '/ce', username='couchbase', password='couchbase')
cb_write.n1ql_timeout = 8000
cb_read.n1ql_timeout = 8000


def read_company_settings(limit=10, offset=0):
    company_settings = []
    query = _N1QLQuery('SELECT META().id,* FROM ce WHERE type=$1', 3)
    query.timeout = 8000
    companies_by_types = N1QLRequest(query, cb_read)
    # companies_by_types = N1QLRequest(
    #     _N1QLQuery('SELECT META().id,* FROM ce WHERE type=$1 order by id LIMIT $2 OFFSET $3', 3, limit, offset), cb)
    for comp_obj in companies_by_types:
        company_settings.append(comp_obj['id'])
    return company_settings

def update_cb_object(comp_id):
    import couchbase_core.subdocument as SD

    medias = ["DOCUMENT_WORD", "DOCUMENT_POWERPOINT", "DOCUMENT_EXCEL", "DOCUMENT_PDF", "AUDIO", "VIDEO"]
    try:
        # r1=cb_write.get(comp_id)
        r1 = cb.get(comp_id)
        value = r1.value
        # mig = value.get('extra_config', {}).get('picassoMigratedMedia','')
        # if len(mig)>0:
        #     mig=mig.split(',')
        # else:
        #     mig=[]
        # new_mig = ",".join(list(set(mig + medias)))
        if value.get('extra_config') is None:
            value['extra_config']={}
        # value['extra_config']['picassoMigratedMedia'] = "VIDEO"
        # value['extra_config']['picassoMedia'] = "VIDEO"
        # value['extra_config']['picassoEnabled'] = True
        value['extra_config']['reflowEnabled'] = True
        # cb_write.upsert(comp_id, value)
        cb.upsert(comp_id, value)
        print(f'Success - {comp_id}')
        # cb.mutate_in(doc_id, SD.upsert("picassoMigratedMedia.test", "newValue", create_parents=True))
        # resp = N1QLRequest(
        # _N1QLQuery('UPDATE ce SET extra_config.picassoMigratedMedia = $1 WHERE META().id=$2', medias, doc_id ), cb)
    except Exception as e:
        print(f'Could not modify cb object - {comp_id} - exception -{e}')


def enable_flags():
    # limit=100
    # offset=0
    # while True:
    #     comp = read_company_settings(limit, offset)
    #     if len(comp)==0:
    #         break
    #     for comp_id in comp:
    #         update_cb_object(comp_id)
    #     offset+=limit
    # print(f'completed')

    comp = read_company_settings()
    for comp_id in comp:
        update_cb_object(comp_id)
    print('completed')

if __name__=='__main__':
    # enable_flags()
    update_cb_object('1341001802051084202.settings')