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

cbhost = 'cb-backup-ce-node-1.internal.mindtickle.com:8091'

cb = Bucket('couchbase://' + cbhost + '/ce', username='couchbase', password='couchbase')

companyTypes = ['CUSTOMER', 'PROSPECT', 'QA', 'DEV', 'UNKNOWN', 'DELETED']

docs = {90: 'AUDIO', 93: 'PDF', 94: 'PPT', 95: 'WORD', 96: 'XLS', 0: 'INVALID'}
mediaTypes = list(docs.keys())

base_name = 'documents_media'
sub_dir = ''

collections = {
  'picasso_media': 'picasso_media',
  'representation_media': 'representation_media',
  'representation_media_properties': 'representations_properties'
}


def get_dir(prefix, sub_dir):
    current_dir = os.getcwd()
    dir = os.path.join(sub_dir, f'{prefix}_companies_media')
    if prefix == '':
        dir = sub_dir
    path = os.path.join(current_dir, dir)
    if not os.path.exists(path):
        os.makedirs(path)
    return path



def load_companies_for_mapping():
    import glob
    print('Loading companies for mapping...')
    path = get_dir("migrated", sub_dir)
    files = glob.glob(path + '/*.csv')
    comp = []
    for fl in files:
        f = fl.split('/')[-1]
        cmp = f.split('.')[0].strip('migrated_')
        comp.append(cmp)
    print('companies loaded...')
    return comp



def getDoc(id, doc):
  return Doc(id=id, doc=doc)


def get_create_doc_requests(docs, collection_id, user_id, tenant_id):
  req_context = RequestContext(user_id=user_id, tenant_id=tenant_id)
  return CreateDocsRequest(request_context= req_context, collection_id=collection_id, doc=docs)


def get_picasso_create_requests(picasso_medias, user_id, tenant_id):

  docs = []
  for each in picasso_medias:
    doc = getDoc(each['id'], json.dumps(each))
    docs.append(doc)
  create_requests = get_create_doc_requests(docs, collections['picasso_media'],user_id, tenant_id)
  return create_requests


def get_representation_media_create_requests(representation_medias, user_id, tenant_id):

  docs = []
  for each in representation_medias:
    doc = getDoc(each['id'], json.dumps(each))
    docs.append(doc)
  create_requests = get_create_doc_requests(docs, collections['representation_media'],user_id, tenant_id)
  return create_requests


def get_representation_properties_media_create_requests(representation_properties, user_id, tenant_id):

  docs = []
  for each in representation_properties:
    doc = getDoc(each['id'], json.dumps(each))
    docs.append(doc)
  create_requests = get_create_doc_requests(docs, collections['representation_media_properties'],user_id, tenant_id)
  return create_requests


def get_representation_name(infra_media_type):

  if infra_media_type == 'IMAGE':
    return 'IMAGE'
  elif infra_media_type == 'VTT':
    return 'AUTO_SUBTITLE'
  elif infra_media_type == 'JSON':
    return 'TRANSCRIPTION'
  elif infra_media_type == 'AUDIO':
    return 'MP3'
  elif infra_media_type == 'PDF':
      return 'PDF'
  elif infra_media_type == 'DOC':
      return 'PDF'
  elif infra_media_type == 'PPT':
      return 'PDF'
  elif infra_media_type == 'XLS':
      return 'PDF'
  if infra_media_type == 'CATALOGUE':
    return 'IMAGE'
  else:
      return 'UNDEFINED'


def generate_representation_id(representation_name, media_id):
  l = ['R', representation_name, media_id]
  # print(l)
  return '-'.join(l)


def generate_representation_properties_id(representation_name, media_id):
  now = datetime.datetime.now()
  time_micro = now.strftime("%y%m%d%H%M%S%f")
  rando = _random.Random()
  value = rando.getrandbits(9)
  l = ['R', representation_name, media_id, time_micro, str(value)]
  return '-'.join(l)


def get_base_media(id, orgId, companyId, userId):
  myMap={}
  myMap["id"] = id
  myMap["orgId"] = orgId
  myMap["companyId"] = companyId
  myMap["tenantId"] = orgId
  myMap["authorizer"] = userId
  myMap["globalContextId"] = myMap["id"]
  return myMap


def get_media(id, orgId, companyId, userId, infraMediaId, name, type):
  media_obj = get_base_media(id, orgId, companyId, userId)
  media_obj['infraMediaId'] = infraMediaId
  media_obj['requestSource'] = 'migration'
  media_obj['type'] = type
  media_obj["requestSource"] = 'content-engine-migration'
  media_obj['name'] = name
  return media_obj


def get_representation(id, orgId, companyId, userId, representationName, mediaId):
  media_obj = get_base_media(id, orgId, companyId, userId)
  media_obj['representationName'] = representationName
  media_obj['sourceType'] = 'sourceType'
  media_obj['mediaId'] = mediaId
  return media_obj


def get_representation_properties(id, orgId, companyId, userId,representation_id, infra_media_id, division):
  media_obj = get_base_media(id, orgId, companyId, userId)
  media_obj['representationId'] = representation_id
  media_obj['infraMediaId'] = infra_media_id
  media_obj['division'] = division
  return media_obj


def get_picasso_type(media_type):
    if media_type == 'AUDIO':
        return 'AUDIO'
    elif media_type == 'IMAGE':
        return 'IMAGE'
    elif media_type == 'WORD':
        return 'DOCUMENT_WORD'
    elif media_type == 'PPT':
        return 'DOCUMENT_POWERPOINT'
    elif media_type == 'XLS':
        return 'DOCUMENT_EXCEL'
    elif media_type == 'PDF':
        return 'DOCUMENT_PDF'
    return 'UNDEFINED'



async def map_company_data(comp_id):
    print(f'Start mapping company media for company - {comp_id}')
    migrated_media = get_dir("migrated", sub_dir)
    if not os.path.exists(f'{migrated_media}/migrated_{comp_id}.csv'):
        print(f'media data does not exists for company - {comp_id}')
        return
    failed_mapping = get_dir("failed_mapping", sub_dir)
    successful_mapping = get_dir("successful_mapping", sub_dir)
    mig_except = 0

    with open(f'{successful_mapping}/successful_mapping_{comp_id}.csv', 'w') as f1, open(f'{failed_mapping}/failed_mapping_{comp_id}.csv','w') as f2, open(f'{migrated_media}/migrated_{comp_id}.csv') as f3:

        success_writer = csv.writer(f1)
        failed_writer = csv.writer(f2)
        media_reader = csv.reader(f3)
        picasso_media_list = []
        representation_media_list = []
        representation_properties_media_list = []
        mapped_medias = []
        medias = []

        for row in media_reader:
            raw_obj = row[0]
            media_obj = json.loads(row[0])
            try:
                mediaId = media_obj['id']
                original_media = media_obj['original_media'][0]
                org_id = media_obj['orgId']
                company_id = media_obj['companyId']
                uploaded_by_id = media_obj['userId']
                user_id = uploaded_by_id
                type = original_media['type']
                if type == 'DOCUMENT':
                    picasso_type = get_picasso_type(original_media['subtype'])
                else:
                    picasso_type = get_picasso_type(type)
                picasso_obj = get_media(mediaId, org_id, company_id, uploaded_by_id, original_media['id'], original_media['name'], picasso_type)
                sub_medias = media_obj['sub_media']
                repr_list=[]
                repr_prop_list=[]
                for media in sub_medias:
                    type_infra = ''
                    division = ''
                    if media['type'] == 'IMAGE':
                        type_infra = 'IMAGE'
                        division = 'THUMBNAIL_LARGE'
                    elif media['type'] == 'AUDIO':
                        type_infra = 'AUDIO'
                        division = 'MP3'
                    elif media['type'] == 'DOCUMENT':
                        type_infra = media['subtype']
                        division = get_representation_name(type_infra)
                    elif media['type'] == 'CATALOGUE':
                        type_infra = 'CATALOGUE'
                        division = 'IMAGE'

                    representation_id = generate_representation_id(get_representation_name(type_infra), mediaId)
                    representation_media_object = get_representation(representation_id, org_id, company_id, uploaded_by_id, get_representation_name(type_infra), mediaId)
                    if len(representation_media_object) > 0:
                        repr_list.append(representation_media_object)
                    representation_properties_id = generate_representation_properties_id(get_representation_name(type_infra), mediaId)
                    representation_property_object = get_representation_properties(representation_properties_id, org_id, company_id, uploaded_by_id, representation_media_object['id'], media['id'], division)
                    if len(representation_property_object) > 0:
                        repr_prop_list.append(representation_property_object)

            except Exception as e:
                failed_writer.writerow([raw_obj, str(e)])
                print(f'Exception occurred while migrating company {comp_id} - media - {media_obj}, Exception - {e}')
                logging.debug(f'Exception occurred while migrating company {comp_id} - media - {media_obj}, Exception - {e}')
                mig_except = 1
                continue

            picasso_media_list.append(picasso_obj)
            representation_media_list.extend(repr_list)
            representation_properties_media_list.extend(repr_prop_list)
            medias.append(raw_obj)
            mapped_obj = {'id': mediaId, 'orgId': org_id, 'companyId': company_id, 'picasso_media': picasso_obj, 'representation_media': repr_list, 'representation_properties_media': repr_prop_list}
            mapped_medias.append(json.dumps(mapped_obj))

            if len(picasso_media_list) == 5:
                try:
                    create_media_request_list = []
                    picasso_medias = get_picasso_create_requests(picasso_media_list, user_id, org_id)
                    create_media_request_list.append(picasso_medias)

                    if len(representation_media_list) > 0:
                        reps_medias = get_representation_media_create_requests(representation_media_list, user_id,org_id)
                        create_media_request_list.append(reps_medias)

                    if len(representation_properties_media_list)>0:
                        reps_property_medias = get_representation_properties_media_create_requests(representation_properties_media_list, user_id, org_id)
                        create_media_request_list.append(reps_property_medias)

                    modifyDocsRequest = ModifyDocsRequest(
                        request_context=RequestContext(user_id=user_id, tenant_id=org_id),
                        create_docs_request=create_media_request_list)
                    resp = stub.ModifyDocs(modifyDocsRequest)
                    success_writer.writerows([[obj] for obj in mapped_medias])

                except Exception as e:
                    failed_writer.writerows([[obj, str(e)] for obj in medias])
                    print(f'create infra media request failed for company {comp_id} - batch - {medias}, Exception - {e}')
                    logging.debug(f'create infra media request failed for company {comp_id} - batch - {medias}, Exception - {e}')
                    mig_except = 1

                picasso_media_list=[]
                representation_media_list=[]
                representation_properties_media_list=[]
                medias=[]
                mapped_medias=[]

        if len(picasso_media_list) > 0:
            try:
                create_media_request_list = []
                picasso_medias = get_picasso_create_requests(picasso_media_list, user_id, org_id)
                create_media_request_list.append(picasso_medias)

                if len(representation_media_list) > 0:
                    reps_medias = get_representation_media_create_requests(representation_media_list, user_id, org_id)
                    create_media_request_list.append(reps_medias)

                if len(representation_properties_media_list) > 0:
                    reps_property_medias = get_representation_properties_media_create_requests(
                        representation_properties_media_list, user_id, org_id)
                    create_media_request_list.append(reps_property_medias)

                modifyDocsRequest = ModifyDocsRequest(
                    request_context=RequestContext(user_id=user_id, tenant_id=org_id),
                    create_docs_request=create_media_request_list)
                resp = stub.ModifyDocs(modifyDocsRequest)
                success_writer.writerows([[obj] for obj in mapped_medias])

            except Exception as e:
                failed_writer.writerows([[obj, str(e)] for obj in medias])
                logging.debug(f'create infra media request failed for company {comp_id} - batch - {medias}, Exception - {e}')
                print(f'create infra media request failed for company {comp_id} - batch - {medias}, Exception - {e}')
                mig_except = 1

    if mig_except == 0 and os.path.exists(f'{failed_mapping}/failed_mapping_{comp_id}.csv'):
        os.remove(f'{failed_mapping}/failed_mapping_{comp_id}.csv')
    print(f'Migration completed for company - {comp_id}')


async def map_media_data_by_company(companies_list=[]):
    # global processed_companies
    # companies_list = processed_companies
    print(f'Start media mapping....')
    tasks = []
    for comp_id in companies_list:
        tasks.append(map_company_data(comp_id))
    await asyncio.gather(*tasks)
    print('Mapping completed')



async def main():

    global sub_dir
    sub_dir = 'Testing11'
    comp_list = load_companies_for_mapping()
    map_task = asyncio.create_task(map_media_data_by_company(comp_list))
    await map_task

    pass



if __name__ == '__main__':
    # main()
    asyncio.run(main())