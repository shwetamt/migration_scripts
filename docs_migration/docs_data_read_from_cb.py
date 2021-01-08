import csv
import copy
import grpc
from couchbase.bucket import Bucket
from couchbase_core.n1ql import _N1QLQuery, N1QLRequest
import datetime, _random
import asyncio
import json
import os
from tickleDb.document_pb2_grpc import DocServiceStub

TICKLE_DB_URL = 'tickledbdocservice.internal-grpc.titos.mindtickle.com:80'
channel = grpc.insecure_channel(TICKLE_DB_URL)
channel_ready_future = grpc.channel_ready_future(channel)
channel_ready_future.result(timeout=10)
stub = DocServiceStub(channel)

cbhost = 'cb-backup-ce-node-1.internal.mindtickle.com:8091'

cb = Bucket('couchbase://' + cbhost + '/ce', username='couchbase', password='couchbase')

companyTypes = ['CUSTOMER', 'PROSPECT', 'QA', 'DEV', 'UNKNOWN', 'DELETED']

docs = {90: 'AUDIO', 93: 'PDF', 94: 'PPT', 95: 'WORD', 96: 'XLS', 0: 'INVALID'}
mediaTypes = list(docs.keys())

base_name = 'documents_media'

sub_dir = ''
failed_db_writer = ''
path_writer = ''


def get_dir(prefix, sub_dir):
    current_dir = os.getcwd()
    dir = os.path.join(sub_dir, f'{prefix}_companies_media')
    if prefix == '':
        dir = sub_dir
    path = os.path.join(current_dir, dir)
    if not os.path.exists(path):
        os.makedirs(path)
    return path


collections = {
    'infra_media': 'infra_media',
}

picasso_bucket = {
    1: 'mt-picasso-asia-singapore',
    2: 'mt-picasso-eu-ireland',
    3: 'mt-picasso-us-east',
}


region_bucket = {
    1: "mtgame-cdn.mindtickle.com",
    2: "mtgame-us.mindtickle.com",
    3: "mtgame-eu.mindtickle.com"
}

streaming_bucket = {
    1: "mtstreaming-cdn.mindtickle.com",
    2: "mtgame-us.mindtickle.com",
    3: "mtgame-eu.mindtickle.com"
}

box_bucket = {
    1: 'mtdocs-box-processed',
    2: "mtdocs-us-box-processed",
    3: "mtdocs-eu-box-processed"
}

croco_bucket = {
    1: "mtdocs-conversion",
    2: "mtdocs-us-conversion",
    3: "mtdocs-eu-conversion"
}

region_cdn = {
    1: "ap-southeast-1",
    2: "us-east-1",
    3: "eu-west-1"
}

failed_companies = set()
companySettings = {}
processed_companies = []


def get_file_name(prefix="", comp_type='QA'):
    if prefix != "":
        return f'{prefix}_{base_name}_{comp_type}.csv'
    return f'{base_name}_{comp_type}.csv'


def get_document_from_bucket(doc_id, bucket):
    return bucket.get(doc_id)


def write_failed_migrations(comp_type):
    with open(get_file_name("failed", comp_type), 'a') as f:
        w = csv.writer(f)
        for comp in failed_companies:
            w.writerow([comp])


def write_processed_migrations(comp_type):
    with open(get_file_name("processed", comp_type), 'a') as f:
        w = csv.writer(f)
        for comp in processed_companies:
            w.writerow([comp])



def read_processed_companies():
    global processed_companies
    for comp_type in companyTypes:
        try:
            with open(get_file_name('processed', comp_type)) as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    processed_companies.add(row)
        except Exception as e:
            pass
        return




def load_companies_to_process():
    import glob
    print(f'Loading companies to migrate...')
    path = get_dir("downloaded", sub_dir)
    files = glob.glob(path + '/*.csv')
    comp = []
    for fl in files:
        f = fl.split('/')[-1]
        cmp = f.split('.')[0].strip('downloaded_')
        comp.append(cmp)
    print('Companies  successfully loaded')
    return comp


def load_company_settings():
    global companySettings

    with open(f'company_settings.csv') as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            id, obj = row[0], json.loads(row[1])
            companySettings[id] = obj
    print(f'company settings loaded...')


def read_company_settings_from_db():
    company_settings = {}
    companies_by_types = N1QLRequest(_N1QLQuery('SELECT META().id,* FROM ce WHERE type=$1', 3), cb)
    for comp_obj in companies_by_types:
        if comp_obj['ce'].get('companyState') != 'ACTIVE':
            continue
        company_settings[comp_obj['id']] = comp_obj['ce']

    with open('company_settings.csv', 'w') as f:
        csv_writer = csv.writer(f)
        for cId, cObj in company_settings.items():
            csv_writer.writerow([cId, json.dumps(cObj)])



failed_db_reads = []

def get_failed_cb_reads_companies():
    comp=set()
    dir_path = get_dir('', sub_dir)
    failed_reads = f'{dir_path}/failed_db_reads.csv'
    if not os.path.exists(failed_reads):
        return set()
    with open(failed_reads) as f:
        reader = csv.reader(f)
        for row in reader:
            comp.add(row[0])
    return comp


def get_offset(file):
    cnt=0
    with open(file) as f:
        reader = csv.reader(f)
        for row in reader:
            cnt+=1
    return cnt


def read_media_by_company(comp_id, offset=0):
    global processed_companies
    print(f'Reading company data for company - {comp_id}')
    path = get_dir('downloaded', sub_dir)
    file = os.path.join(path, f'downloaded_{comp_id}.csv')
    if os.path.exists(file):
        offset = get_offset(file)
    status = [0, 5 ,6 ,22]
    thresh = 1000
    with open(file, 'a') as f:
        csv_writer = csv.writer(f)
        while True:
            limit = offset+thresh
            media_records = []
            try:
                medias = N1QLRequest(
                    _N1QLQuery(
                        'SELECT META().id, * FROM ce WHERE companyId=$1 and type in $2 order by id LIMIT $3 OFFSET $4',
                        comp_id, mediaTypes, limit,
                        offset), cb)
                if medias.metrics.get('resultCount', 0) == 0 :
                    break
                for media_obj in medias:
                    if media_obj['ce'].get('status', -1) not in status:
                        continue
                    media_records.append([json.dumps(media_obj)])

            except Exception as e:
                failed_db_writer.writerow([comp_id, offset])
                return

            csv_writer.writerows(media_records)
            offset = limit

    print(f'Finished Reading company data for company - {comp_id}')



def get_media_count_by_company(comp_type):
    comp_list = get_companies_by_type(comp_type)
    comp_map = {}
    for comp in comp_list:
        medias = N1QLRequest(
            _N1QLQuery(
                'SELECT count(*) as cnt FROM ce WHERE companyId=$1 and type in $2', comp, mediaTypes), cb)
        # if medias.metrics.get('resultCount', 0) == 0:
        #     break
        medias = list(medias)[0]
        comp_map[comp]=medias.get('cnt',0)

    with open(f'{comp_type}_companies_cnt.csv', 'w') as f:
        writer = csv.writer(f)
        for comp in sorted(comp_map, key=comp_map.get):
            writer.writerow([comp, comp_map[comp]])



def get_companies_by_type(comp_type='ALL'):
    print(f'Getting companies by type - {comp_type}')
    if comp_type == 'ALL':
        return [comp.strip('.settings') for comp in companySettings.keys()]

    if comp_type not in companyTypes:
        return []

    companies = [comp.strip('.settings') for comp in companySettings if
                 companySettings[comp].get('companyType') == comp_type]
    return companies


def read_batch_to_migrate_from_db(comp_list):
    print('Start Reading medias from db....')
    for comp in comp_list:
        if companySettings.get(f'{comp}.settings') is not None:
            read_media_by_company(comp)
    print(f'Finieshed Reading data from db')

    # for comp in companies_list[:20]:
    #   read_media_by_company(comp)


# def documents_data_migration():
#   for comp_type in companyTypes:
#     data_mig(comp_type)
#   # res = await asyncio.gather(*[data_mig(comp_type) for comp_type in companyTypes])


def read_data_for_migration(comp_list, dir):
    # read_company_settings_from_db()
    load_company_settings()
    #
    # comp_list = get_companies_by_type('CUSTOMER')
    # comp_list = comp_list[400:405]
    # for type in companyTypes:
    #     get_media_count_by_company(type)

    # comp_list = ['817283497610854710']

    global sub_dir, failed_db_writer
    sub_dir = dir
    dir_path = get_dir('', sub_dir)
    failed_reads = open(f'{dir_path}/failed_db_reads.csv','a')
    failed_db_writer=csv.writer(failed_reads)

    read_batch_to_migrate_from_db(comp_list)
    failed_reads.close()



# if __name__ == '__main__':
#     main()
