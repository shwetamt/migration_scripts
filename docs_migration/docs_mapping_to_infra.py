import csv
import copy
import grpc
from couchbase.bucket import Bucket
from couchbase_core.n1ql import _N1QLQuery, N1QLRequest
import datetime, _random
import asyncio
import json
import os
import logging
from pathlib import Path
from common.common_messages_pb2 import RequestContext
from tickleDb.document_pb2 import ModifyDocsRequest, CreateDocsRequest, Doc
from tickleDb.document_pb2_grpc import DocServiceStub
import shutil

TICKLE_DB_URL = 'tickledbdocservice.internal-grpc.titos.mindtickle.com:80'
channel = grpc.insecure_channel(TICKLE_DB_URL)
channel_ready_future = grpc.channel_ready_future(channel)
channel_ready_future.result(timeout=10)
stub = DocServiceStub(channel)

# cbhost = '10.11.120.220:8091'
# cb = Bucket('couchbase://' + cbhost + '/ce', username='mindtickle', password='testcb6mindtickle')

cbhost = 'cb6-node-1-staging.mindtickle.com:8091'
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


def generateId(media_type):
    now = datetime.datetime.now()
    time_micro = now.strftime("%y%m%d%H%M%S%f")
    rando = _random.Random()
    value = rando.getrandbits(9)
    l = [media_type, time_micro, str(value)]
    val = "".join(l)
    return val


def get_media_object(cb_obj, comp_obj, media_type = ''):
    media_obj = cb_obj['ce']
    type = media_obj['type']
    user_id = media_obj.get('uploadedById', '_default_docs_migration')
    if user_id is None or user_id=='':
        user_id = '_default_docs_migration'
    cdnId = comp_obj['cdnId']
    orgId = comp_obj["orgId"]
    company_id = media_obj['companyId']
    if media_type == '':
        media_type = docs[type]

    mapped_obj = {}
    mapped_obj["id"] = generateId(media_type)
    mapped_obj["globalContextId"] = mapped_obj["id"]
    mapped_obj["orgId"] = orgId
    mapped_obj["companyId"] = company_id
    mapped_obj["tenantId"] = orgId
    mapped_obj["authorizer"] = user_id
    mapped_obj["source"] = 'content-engine-migration'
    mapped_obj["region"] = region_cdn[cdnId]
    mapped_obj['name'] = media_obj.get('title', '')
    return mapped_obj


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


def getDoc(id, doc):
    return Doc(id=id, doc=doc)


def get_audio_mime(path):
    ext = path.split('.')
    if len(ext) < 2:
        return '.mp3'
    ext = ext[-1]
    if ext == 'mp3':
        return ".mp3"
    elif ext == "flac":
        return ".flac"
    return ".mp3"


def get_audio_objects(cb_obj):
    mapped_dict = {'original_media': [], 'sub_media': []}
    media_obj = cb_obj['ce']
    cId = media_obj.get('companyId', "")
    comp_obj = companySettings.get(f'{cId}.settings')
    if comp_obj is None:
        return []
    cdn = comp_obj['cdnId']

    # m = get_media_object(cb_obj, comp_obj)
    if 'original_path' not in media_obj or media_obj['original_path'] is None:
        raise Exception(f"original path not present in company - {cId}")

    mapped_dict['original_media'].append(get_mapped_audio_obj(get_media_object(cb_obj, comp_obj, 'AUDIO'), media_obj, get_audio_mime(media_obj.get('original_path')), region_bucket[cdn], media_obj.get('original_path'), cdn))

    if 'mp3Path' in media_obj and media_obj['mp3Path'] is not None:
        mapped_dict['sub_media'].append(get_mapped_audio_obj(get_media_object(cb_obj, comp_obj, 'AUDIO'), media_obj, ".mp3", streaming_bucket[cdn], media_obj.get('mp3Path'), cdn))

    if 'flacPath' in media_obj and media_obj['flacPath'] is not None:
        mapped_dict['sub_media'].append(get_mapped_audio_obj(get_media_object(cb_obj, comp_obj, 'AUDIO'), media_obj, ".flac", region_bucket[cdn], media_obj.get('flacPath'), cdn))

    if 'vttSubtitlePath' in media_obj and media_obj['vttSubtitlePath'] is not None:
        mapped_dict['sub_media'].append(get_subtitle_media(get_media_object(cb_obj, comp_obj, 'DOCUMENT'), media_obj, region_bucket[cdn], media_obj.get('vttSubtitlePath'), cdn))

    # if 'flacPath' in media_obj and media_obj['flacPath'] is not None:
    #   mobj = copy.deepcopy(mapped_obj)
    #   mapped_list.append(get_mapped_audio_obj(mobj, media_obj, media_obj.get('flacPath')))

    if 'transcriptPath' in media_obj and media_obj['transcriptPath'] is not None:
        mapped_dict['sub_media'].append(
            get_transcription_media(get_media_object(cb_obj, comp_obj, 'DOCUMENT'), media_obj, region_bucket[cdn], media_obj.get('transcriptPath'), cdn))
    #
    # if (audio.vttSubtitlePath != null) {
    # audio.vttSubtitlePath = generateCloudFrontUrlString(region, audio.vttSubtitlePath, BucketType.OTHERS, expiry);
    # }
    # if (audio.flacPath != null) {
    # audio.flacPath = generateCloudFrontUrlString(region, audio.flacPath, BucketType.OTHERS, expiry);
    # }
    # if (audio.transcriptPath != null) {
    # audio.transcriptPath = generateCloudFrontUrlString(region, audio.transcriptPath, BucketType.OTHERS, expiry);
    # }
    return mapped_dict


def get_audio_type(ext):
    if ext == 'mp3':
        return ".mp3"
    elif ext == 'flac':
        return ".flac"
    return "UNDEFINED"



def get_mapped_audio_obj(mapped_obj, media_obj, mime_key, bucket, s3key, cdn):
    comp_id = mapped_obj['companyId']
    org_id = mapped_obj['orgId']
    id = mapped_obj['id']
    file = s3key.split('/')[-1]
    new_key = f'{org_id}/{comp_id}/{id}/{file}'
    new_bucket = picasso_bucket[cdn]
    path_writer.writerow([bucket, s3key, new_bucket, new_key])
    mapped_obj['original_path'] = s3key
    mapped_obj["key"] = new_key
    mapped_obj["bucket"] = new_bucket
    mapped_obj["type"] = 'AUDIO'
    # mapped_obj["subtype"] = get_audio_type(s3key.split('.')[-1])
    mapped_obj["sizeBytes"] = str(media_obj.get('size', 0))
    mapped_obj["height"] = ''
    mapped_obj["width"] = ''
    mapped_obj["numPages"] = ''
    mapped_obj["locationStatus"] = "SUCCESS"
    mapped_obj["metadataStatus"] = "SUCCESS"
    mapped_obj["locationError"] = ''
    mapped_obj["metadataError"] = ''
    mapped_obj["durationSeconds"] = str(media_obj.get('contentParts', 0))
    # mapped_obj["srcS3Key"]
    mapped_obj["mimeType"] = mime_key    #ext .flac .mp3
    mapped_obj["language"] = ''
    return mapped_obj


def get_image_type(path):
    ext = path.split('.')
    if len(ext)<2:
        return 'UNDEFINED'
    return ext[-1]



def get_mapped_document_obj(mapped_obj, media_obj, mime_type, bucket, s3key, cdn):
    comp_id = mapped_obj['companyId']
    org_id = mapped_obj['orgId']
    id = mapped_obj['id']
    file = s3key.split('/')[-1]
    new_key = f'{org_id}/{comp_id}/{id}/{file}'
    new_bucket = picasso_bucket[cdn]
    path_writer.writerow([bucket, s3key, new_bucket, new_key])
    mapped_obj['original_path'] = s3key
    mapped_obj["key"] = new_key
    mapped_obj["bucket"] = new_bucket
    mapped_obj["type"] = 'DOCUMENT'
    # mapped_obj["subtype"] = sub_type
    mapped_obj["sizeBytes"] = str(media_obj.get('size', 0))
    mapped_obj["numPages"] = str(media_obj.get('contentParts', 0))
    mapped_obj["height"] = ''
    mapped_obj["width"] = ''
    mapped_obj["locationStatus"] = 'SUCCESS'
    mapped_obj["metadataStatus"] = 'SUCCESS'
    mapped_obj["locationError"] = ''
    mapped_obj["metadataError"] = ''
    mapped_obj["mimeType"] = mime_type
    mapped_obj["language"] = ''
    return mapped_obj


def get_mapped_image_object(mapped_obj, media_obj, bucket, s3key, cdn):
    comp_id = mapped_obj['companyId']
    org_id = mapped_obj['orgId']
    id = mapped_obj['id']
    file = s3key.split('/')[-1]
    new_key = f'{org_id}/{comp_id}/{id}/{file}'
    new_bucket = picasso_bucket[cdn]
    path_writer.writerow([bucket, s3key, new_bucket, new_key])
    mapped_obj['original_path'] = s3key
    mapped_obj["key"] = new_key
    mapped_obj["bucket"] = new_bucket
    mapped_obj["type"] = 'IMAGE'
    mapped_obj["subtype"] = s3key.split('.')[-1]
    mapped_obj["sizeBytes"] = str(media_obj.get('size', 0))
    mapped_obj["height"] = '0'
    mapped_obj["width"] = '0'
    mapped_obj["numPages"] = ''
    mapped_obj["locationStatus"] = 'SUCCESS'
    mapped_obj["metadataStatus"] = 'SUCCESS'
    mapped_obj["locationError"] = ''
    mapped_obj["metadataError"] = ''
    mapped_obj["mimeType"] = '.'+ get_image_type(s3key)
    return mapped_obj


def get_mapped_catalogue_object(mapped_obj, media_obj, bucket, s3key, cdn):
    comp_id = mapped_obj['companyId']
    org_id = mapped_obj['orgId']
    id = mapped_obj['id']
    file = s3key.split('/')[-1]
    new_key = f'{org_id}/{comp_id}/{id}/out_'+ '{image_num}.png'
    new_bucket = picasso_bucket[cdn]
    path_writer.writerow([bucket, s3key, new_bucket, new_key])
    mapped_obj['original_path'] = s3key
    mapped_obj["key"] = new_key
    mapped_obj["bucket"] = new_bucket
    mapped_obj["type"] = 'CATALOGUE'
    # mapped_obj["subtype"] = ''
    mapped_obj["sizeBytes"] = '0'
    mapped_obj["height"] = ''
    mapped_obj["width"] = ''
    mapped_obj["numPages"] = str(media_obj.get('contentParts', 0))
    mapped_obj["locationStatus"] = "SUCCESS"
    mapped_obj["metadataStatus"] = "SUCCESS"
    mapped_obj["locationError"] = ''
    mapped_obj["metadataError"] = ''
    mapped_obj["mimeType"] = 'CATALOGUE'
    return mapped_obj


def get_subtitle_media(mapped_obj, media_obj, bucket, s3key, cdn):
    comp_id = mapped_obj['companyId']
    org_id = mapped_obj['orgId']
    id = mapped_obj['id']
    file = s3key.split('/')[-1]
    new_key = f'{org_id}/{comp_id}/{id}/{file}'
    new_bucket = picasso_bucket[cdn]
    path_writer.writerow([bucket, s3key, new_bucket, new_key])
    mapped_obj['original_path'] = s3key
    mapped_obj["key"] = new_key
    mapped_obj["bucket"] = new_bucket
    mapped_obj["type"] = 'DOCUMENT'
    # mapped_obj["subtype"] = 'VTT'
    mapped_obj["sizeBytes"] = '0'
    mapped_obj["width"] = ''
    mapped_obj["numPages"] = '1'
    mapped_obj["locationStatus"] = 'SUCCESS'
    mapped_obj["metadataStatus"] = 'SUCCESS'
    mapped_obj["locationError"] = ''
    mapped_obj["metadataError"] = ''
    mapped_obj["language"] = 'en-us'
    mapped_obj["mimeType"] = '.vtt'
    return mapped_obj


def get_transcription_media(mapped_obj, media_obj, bucket, s3key, cdn):
    comp_id = mapped_obj['companyId']
    org_id = mapped_obj['orgId']
    id = mapped_obj['id']
    file = s3key.split('/')[-1]
    new_key = f'{org_id}/{comp_id}/{id}/{file}'
    new_bucket = picasso_bucket[cdn]
    path_writer.writerow([bucket, s3key, new_bucket, new_key])
    mapped_obj['original_path'] = s3key
    mapped_obj["key"] = new_key
    mapped_obj["bucket"] = new_bucket
    mapped_obj["type"] = 'DOCUMENT'
    # mapped_obj["subtype"] = 'JSON'
    mapped_obj["sizeBytes"] = '0'
    mapped_obj["width"] = ''
    mapped_obj["numPages"] = '1'
    mapped_obj["locationStatus"] = 'SUCCESS'
    mapped_obj["metadataStatus"] = 'SUCCESS'
    mapped_obj["locationError"] = ''
    mapped_obj["metadataError"] = ''
    mapped_obj["mimeType"] = '.json'
    return mapped_obj


def get_mime_by_type(type):
    if type == 'PDF':
        return ".pdf"
    elif type == "WORD":
        return ".doc"
    elif type == "XLS":
        return ".xls"
    elif type == "PPT":
        return ".ppt"
    return ".pdf"


def get_doc_mime(path, type):
    ext = path.split('.')
    if len(ext) < 2:
        return get_mime_by_type(type)
    ext = ext[-1]
    if ext == 'pdf':
        return ".pdf"
    elif ext == "vtt":
        return "vtt"
    elif ext == 'ppt' or ext == 'pptx':
        return ".ppt"
    elif ext == 'doc' or ext == 'docx':
        return ".doc"
    elif ext == 'json' or ext == '.out':
        return ".json"
    elif ext == "xlsx" or ext == 'xls':
        return ".xls"
    return get_mime_by_type(type)


def get_document_objects(cb_obj):

    obj_id, media_obj = cb_obj['id'], cb_obj['ce']
    mapped_dict = {'original_media': [], 'sub_media': []}
    media_obj = cb_obj['ce']
    cId = media_obj.get('companyId', "")
    comp_obj = companySettings.get(f'{cId}.settings')
    if comp_obj is None:
        return []
    cdn = comp_obj['cdnId']
    type = docs.get(media_obj['type'], 'PDF')

    if 'original_path' not in media_obj or media_obj['original_path'] is None:
        raise Exception(f"original path not present in company - {cId}")

    original_path = media_obj['original_path']
    mapped_dict['original_media'].append(get_mapped_document_obj(get_media_object(cb_obj, comp_obj, 'DOCUMENT'), media_obj, get_doc_mime(original_path, type), region_bucket[cdn], original_path, cdn))

    if media_obj.get('uuid') or media_obj.get('docProcessor') == 'BOX' or media_obj.get('docProcessor') == 'HYBRID':

        if media_obj.get('docProcessor') == 'CROCODOC' or media_obj.get('docProcessor') is None:
            uuid = media_obj.get('uuid', '')
            pdf = get_mapped_document_obj(get_media_object(cb_obj, comp_obj, 'DOCUMENT'), media_obj, '.pdf', croco_bucket[cdn],
                                          uuid + "/doc.pdf", cdn)
            thumb = get_mapped_image_object(get_media_object(cb_obj, comp_obj, 'IMAGE'), media_obj, croco_bucket[cdn],
                                            uuid + "/images/thumbnail-master-0.png", cdn)
            mapped_dict['sub_media'].append(pdf)
            mapped_dict['sub_media'].append(thumb)

        else:
            pdf = get_mapped_document_obj(get_media_object(cb_obj, comp_obj, 'DOCUMENT'), media_obj, '.pdf',box_bucket[cdn],
                                          obj_id + "/doc.pdf", cdn)
            thumb = get_mapped_image_object(get_media_object(cb_obj, comp_obj, 'IMAGE'), media_obj, box_bucket[cdn],
                                            obj_id + "/images/thumbnail-master-0.png", cdn)
            mapped_dict['sub_media'].append(pdf)
            mapped_dict['sub_media'].append(thumb)

    if media_obj.get('docProcessor') == 'HTML_PDF_LAMBDA':
        pdf = get_mapped_document_obj(get_media_object(cb_obj, comp_obj, 'DOCUMENT'), media_obj, '.pdf',
                                      region_bucket[cdn],
                                      original_path + "/doc.pdf", cdn)
        thumb = get_mapped_image_object(get_media_object(cb_obj, comp_obj, 'IMAGE'), media_obj, croco_bucket[cdn],
                                        original_path + "/imagified/out_1.png", cdn)
        mapped_dict['sub_media'].append(pdf)
        mapped_dict['sub_media'].append(thumb)

    if media_obj.get('imagifiedStatus') == "IMAGIFIED_SUCCESS":
        # content_parts = media_obj.get('contentParts', 0)
        catalogues_obj = get_mapped_catalogue_object(get_media_object(cb_obj, comp_obj, 'CATALOGUE'), media_obj,
                                                     region_bucket[cdn],
                                                     f'{original_path}/imagified/out_' + '{image_num}.png', cdn)
        mapped_dict['sub_media'].append(catalogues_obj)

    return mapped_dict


def get_mapped_media_objects(cb_obj):
    type = cb_obj['ce']['type']
    if docs[type] == 'AUDIO':
        mapped_objs = get_audio_objects(cb_obj)
    else:
        mapped_objs = get_document_objects(cb_obj)
    return mapped_objs


def get_create_requests(mapped_docs, user_id, tenant_id):
    docs = []
    req_context = RequestContext(user_id=user_id, tenant_id=tenant_id)
    for doc in mapped_docs:
        docs.append(getDoc(doc['id'], json.dumps(doc)))
    return CreateDocsRequest(request_context=req_context, collection_id=collections['infra_media'], doc=docs)



# def migrate_media(compType):
#   processed_media = set()
#   failed_media = set()
#   create_requests_list = []
#   with open(get_file_name("", compType)) as f:
#     reader = csv.reader(f)
#     for row in reader:
#       try:
#         obj_id, media_obj = row[0], json.loads(row[1])
#         if obj_id in processed_media:
#           continue
#         create_requests = get_create_requests(media_obj)
#         create_requests_list.append(create_requests)
#
#         if len(create_requests_list) == 10:
#           modifyDocsRequest = ModifyDocsRequest(request_context=getRequestContext('', orgId),
#                                                 create_docs_request=create_requests_list)
#           stub.ModifyDocs(modifyDocsRequest)
#           processed_media.add(obj_id)
#           create_requests_list = []
#
#       except Exception as e:
#         failed_media.add(obj_id)

def get_already_processed_media(comp_id):
    already_processed = set()
    migrated_media = get_dir("migrated_infra", sub_dir)
    if not os.path.exists(f'{migrated_media}/migrated_infra_{comp_id}.csv'):
        return already_processed
    with open(f'{migrated_media}/migrated_infra_{comp_id}.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            obj=json.loads(row[0])
            already_processed.add(obj['id'])
    return already_processed


async def migrate_company(comp_id):
    print(f'Start migrating company - {comp_id}')
    downloaded_media = get_dir("downloaded", sub_dir)
    if not os.path.exists(f'{downloaded_media}/downloaded_{comp_id}.csv'):
        print(f'media does not exist for comp - {comp_id}')
        return
    status = [0, 5, 6, 22]
    global path_writer
    dir = get_dir('object_paths', sub_dir)
    obj_paths_file = open(f'{dir}/object_paths_{comp_id}.csv', 'a')
    path_writer = csv.writer(obj_paths_file)
    failed_media = get_dir("failed_infra", sub_dir)
    migrated_media = get_dir("migrated_infra", sub_dir)
    mig_except = 0
    already_procesed = get_already_processed_media(comp_id)

    with open(f'{migrated_media}/migrated_infra_{comp_id}.csv', 'a') as f1, open(f'{failed_media}/failed_infra_{comp_id}.csv', 'w') as f2, open(f'{downloaded_media}/downloaded_{comp_id}.csv') as f3:

        mig_writer = csv.writer(f1)
        failed_writer = csv.writer(f2)
        comp_reader = csv.reader(f3)
        create_requests_list = []
        medias = []
        mapped_medias = []

        for row in comp_reader:
            raw_obj = row[0]
            cb_obj = json.loads(row[0])
            if cb_obj['id'] in already_procesed:
                continue
            if cb_obj['ce'].get('status', 0) not in status:
                continue
            try:
                ce_obj = cb_obj['ce']
                comp_obj = companySettings.get(f'{comp_id}.settings')
                if comp_obj is None:
                    continue
                user_id = ce_obj.get('uploadedById', '_default_migrated')
                if user_id is None or user_id == '':
                    user_id = '_default_docs_migration'
                orgId = comp_obj["orgId"]
                company_id = ce_obj['companyId']
                mapped_docs = get_mapped_media_objects(cb_obj)
                create_requests = get_create_requests(mapped_docs['original_media']+mapped_docs['sub_media'], user_id, orgId)

            except Exception as e:
                failed_writer.writerow([raw_obj, str(e)])
                print(f'Exception occurred while migrating company {comp_id} - media - {cb_obj}, Exception - {e}')
                logging.debug(f'Exception occurred while migrating company {comp_id} - media - {cb_obj}, Exception - {e}')
                mig_except = 1
                continue

            create_requests_list.append(create_requests)
            medias.append(raw_obj)
            mapped_docs['id'] = cb_obj['id']
            mapped_docs['orgId'] = orgId
            mapped_docs['companyId'] = comp_id
            mapped_docs['userId'] = user_id
            mapped_medias.append(json.dumps(mapped_docs))

            if len(create_requests_list) == 10:
                try:
                    modifyDocsRequest = ModifyDocsRequest(
                        request_context=RequestContext(user_id=user_id, tenant_id=orgId),
                        create_docs_request=create_requests_list)
                    resp = stub.ModifyDocs(modifyDocsRequest)
                    # if resp:
                    #     processed_companies.add(media_obj['id'])
                    # else:
                    #     raise Exception("create media failed")
                    mig_writer.writerows([[obj] for obj in mapped_medias])

                except Exception as e:
                    failed_writer.writerows([[obj, str(e)] for obj in medias])
                    print(f'create infra media request failed for company {comp_id} - batch - {medias}, Exception - {e}')
                    logging.debug(f'create infra media request failed for company {comp_id} - batch - {medias}, Exception - {e}')
                    mig_except=1

                create_requests_list = []
                medias = []
                mapped_medias = []

        if len(create_requests_list)>0:
            try:
                modifyDocsRequest = ModifyDocsRequest(
                    request_context=RequestContext(user_id=user_id, tenant_id=orgId),
                    create_docs_request=create_requests_list)
                resp = stub.ModifyDocs(modifyDocsRequest)
                # if resp:
                #     processed_companies.add(media_obj['id'])
                # else:
                #     raise Exception("create media failed")
                mig_writer.writerows([[obj] for obj in mapped_medias])

            except Exception as e:
                failed_writer.writerows([[obj, str(e)] for obj in medias])
                print(f'create infra media request failed for company {comp_id} - batch - {medias}, Exception - {e}')
                logging.debug(f'create infra media request failed for company {comp_id} - batch - {medias}, Exception - {e}')
                mig_except = 1

    obj_paths_file.close()
    if mig_except==0 and os.path.exists(f'{failed_media}/failed_infra_{comp_id}.csv'):
        os.remove(f'{failed_media}/failed_infra_{comp_id}.csv')

    print(f'Migration completed for company - {comp_id}')

    # if os.path.exists(f'{downloaded_media}/downloaded_{comp_id}.csv'):
    #     os.remove(f'{downloaded_media}/downloaded_{comp_id}.csv')




# async def companies_migration(type='QA'):
#   with open(get_file_name("", type)) as f:
#     csv_reader = csv.reader(f)
#     # for row in csv_reader:
#     #   migrate_company(row[0], type)
#     await asyncio.gather(*[migrate_company(cmp[0], type) for cmp in csv_reader])



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


# async def docs_data_mig(compType='QA'):
#
#   company_settings = {}
#   companies_list = []
#   # doc=cb.get('997213202392614482.settings')
#   companies_by_types = N1QLRequest(_N1QLQuery('SELECT META().id,* FROM ce WHERE ce.companyType=$1', compType), cb)
#   for comp_obj in companies_by_types:
#     # if comp_obj['ce'].get('companyState', 'ACTIVE') != 'ACTIVE':
#     #   continue
#     cmp_id = comp_obj.get('id', "").split('.')
#     if not (len(cmp_id) == 2 and cmp_id[1] == 'settings'):
#       continue
#     cmp_id=cmp_id[0]
#     company_settings[cmp_id] = comp_obj['ce']
#     companies_list.append([cmp_id])
#
#   with open(get_file_name("", compType), 'w') as f:
#     csv_writer = csv.writer(f)
#     csv_writer.writerows(companies_list)
#
#
#   with open('company_settings.csv', 'a') as f:
#     csv_writer = csv.writer(f)
#     for cId, cObj in company_settings.items():
#       csv_writer.writerow([cId, json.dumps(cObj)])


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
    fl = f'{sub_dir}/company_settings.csv'
    with open(fl) as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            id, obj = row[0], json.loads(row[1])
            companySettings[id] = obj
    print(f'company settings loaded...')



async def migrate_media_by_company(companies_list=[]):
    # global processed_companies
    # companies_list = processed_companies
    print(f'Start migrating companies to infra media')
    tasks = []
    for comp_id in companies_list:
        if companySettings.get(f'{comp_id}.settings') is None:
            continue
        tasks.append(migrate_company(comp_id))
    await asyncio.gather(*tasks)
    print(f'Migration completed')


failed_db_reads = []


def get_companies_by_type(comp_type='ALL'):
    print(f'Getting companies by type - {comp_type}')
    if comp_type == 'ALL':
        return [comp.strip('.settings') for comp in companySettings.keys()]

    if comp_type not in companyTypes:
        return []

    companies = [comp.strip('.settings') for comp in companySettings if
                 companySettings[comp].get('companyType') == comp_type]
    return companies



# def documents_data_migration():
#   for comp_type in companyTypes:
#     data_mig(comp_type)
#   # res = await asyncio.gather(*[data_mig(comp_type) for comp_type in companyTypes])


async def map_medias_to_infra(comp_list, dir):
    global sub_dir, failed_db_writer, path_writer, companySettings
    sub_dir = dir
    # read_company_settings_from_db()
    load_company_settings()
    # n = f'{comp_list[0]}.settings'
    # r1 = cb.get(n)
    # r1.value['orgId']+='444555'
    # companySettings = {n : r1.value}



    #comp_list = ['595584933866000779']

    # comp_list = load_companies_to_process()

    mig_task = asyncio.create_task(migrate_media_by_company(comp_list))
    await mig_task
    pass


    # read_processed_companies()

    # write_processed_migrations(comp_type)
    # write_failed_migrations(comp_type)


# if __name__ == '__main__':
#     # main()
#     asyncio.run(map_medias_to_infra())
