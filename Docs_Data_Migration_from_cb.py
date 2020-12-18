import csv
import copy
import grpc
from couchbase.bucket import Bucket
from couchbase_core.n1ql import _N1QLQuery, N1QLRequest
import datetime, _random
import asyncio
import json
import os
from common.common_messages_pb2 import RequestContext
from tickleDb.document_pb2 import ModifyDocsRequest, CreateDocsRequest, Doc
from tickleDb.document_pb2_grpc import DocServiceStub

TICKLE_DB_URL = 'tickledbdocservice.internal-grpc.pikachu.mindtickle.com:80'
channel = grpc.insecure_channel(TICKLE_DB_URL)
channel_ready_future = grpc.channel_ready_future(channel)
channel_ready_future.result(timeout=10)
stub = DocServiceStub(channel)

cbhost = 'cb-backup-ce-node-1.internal.mindtickle.com:8091'

cb = Bucket('couchbase://' + cbhost + '/ce', username='couchbase', password='couchbase')

companyTypes = ['CUSTOMER','PROSPECT','QA','DEV','UNKNOWN','DELETED']


docs = {90: 'AUDIO', 93: 'PDF', 94: 'PPT', 95: 'WORD', 96: 'XLS', 0: 'INVALID'}
mediaTypes = list(docs.keys())

base_name = 'documents_media'


def get_dir(prefix):
  parent_dir = os.getcwd()
  dir = f'{prefix}_compaies_media'
  path = os.path.join(parent_dir, dir)
  if not os.path.exists(path):
    os.makedirs(path)
  return path


collections = {
  'infra_media': 'infra_media',
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


def get_media_object(media_type):
  media = {}
  media["id"] = generateId(docs[media_type])
  return media


def get_file_name(prefix="", comp_type='QA'):
  if prefix!="":
    return f'{prefix}_{base_name}_{comp_type}.csv'
  return f'{base_name}_{comp_type}.csv'


def get_document_from_bucket(doc_id, bucket):
  return bucket.get(doc_id)


def write_failed_migrations(comp_type):
  with open(get_file_name("failed", comp_type), 'a') as f:
    w=csv.writer(f)
    for comp in failed_companies:
      w.writerow([comp])


def write_processed_migrations(comp_type):
  with open(get_file_name("processed", comp_type), 'a') as f:
    w=csv.writer(f)
    for comp in processed_companies:
      w.writerow([comp])



def getDoc(id, doc):
  return Doc(id=id, doc=doc)



def get_audio_objects(mapped_obj, cb_obj, cdn):
  mapped_list = []
  media_obj = cb_obj['ce']

  if 'original_path' in media_obj and media_obj['original_path'] is not None:
    mobj = copy.deepcopy(mapped_obj)
    mapped_list.append(get_mapped_audio_obj(mobj, media_obj, region_bucket[cdn], media_obj.get('origional_path')))

  if 'mp3Path' in media_obj and media_obj['mp3Path'] is not None:
    mobj = copy.deepcopy(mapped_obj)
    mapped_list.append(get_mapped_audio_obj(mobj, media_obj, region_bucket[cdn], media_obj.get('mp3Path')))

  if 'vttSubtitlePath' in media_obj and media_obj['vttSubtitlePath'] is not None:
    mobj = copy.deepcopy(mapped_obj)
    mapped_list.append(get_subtitle_media(mobj, media_obj, region_bucket[cdn],  media_obj.get('vttSubtitlePath')))

  # if 'flacPath' in media_obj and media_obj['flacPath'] is not None:
  #   mobj = copy.deepcopy(mapped_obj)
  #   mapped_list.append(get_mapped_audio_obj(mobj, media_obj, media_obj.get('flacPath')))

  if 'transcriptPath' in media_obj and media_obj['transcriptPath'] is not None:
    mobj = copy.deepcopy(mapped_obj)
    mapped_list.append(get_transcription_media(mobj, media_obj, region_bucket[cdn], media_obj.get('vttSubtitlePath')))

  return mapped_list


def get_audio_type(ext):
  if ext=='mp3':
    return "MP3"
  return "UNDEFINED"


def get_mapped_audio_obj(mapped_obj, media_obj, bucket, s3key):

  mapped_obj["key"] = s3key
  mapped_obj["bucket"] = bucket
  mapped_obj["type"] = 'AUDIO'
  mapped_obj["subtype"] = get_audio_type(s3key.split('.')[-1])
  mapped_obj["sizeBytes"] = str(media_obj.get('size', 0))
  # mapped_obj["height"]
  # mapped_obj["width"]
  # mapped_obj["numPages"]
  mapped_obj["locationStatus"] = "SUCCESS"
  mapped_obj["metadataStatus"] = "SUCCESS"
  mapped_obj["locationError"] = ''
  mapped_obj["metadataError"] = ''
  mapped_obj["durationSeconds"] = str(media_obj['contentParts'])
  # mapped_obj["srcS3Key"]
  # mapped_obj["source"]
  mapped_obj["mimeType"] = 'migrated'
  # mapped_obj["language"]
  return mapped_obj


def get_document_type(ext):
  if ext=='pdf':
    return "PDF"
  elif ext=="vtt":
    return "VTT"
  elif ext=='ppt' or ext=='pptx':
    return "PPT"
  elif ext=='doc' or ext=='docx':
    return "DOC"
  elif ext=='json':
    return "JSON"
  elif ext=="XLS":
    return "XLS"
  return "UNDEFINED"


def get_mapped_document_obj(mapped_obj, media_obj, bucket, s3key):
  mapped_obj["key"] =s3key
  mapped_obj['bucket'] = bucket
  mapped_obj["type"] = 'DOCUMENT'
  mapped_obj["subtype"] = get_document_type(s3key.split('.')[-1])
  mapped_obj["sizeBytes"] = str(media_obj.get('size',0))
  mapped_obj["numPages"] = str(media_obj['contentParts'])
  mapped_obj["locationStatus"] = 'SUCCESS'
  mapped_obj["metadataStatus"] = 'SUCCESS'
  mapped_obj["locationError"] = ''
  mapped_obj["metadataError"] = ''
  mapped_obj["mimeType"] = '.'+s3key.split('.')[-1]
  mapped_obj["language"] = ''
  return mapped_obj



def get_mapped_image_object(mapped_obj, media_obj, bucket, s3key):
  mapped_obj["key"] = s3key
  mapped_obj["bucket"] = bucket
  mapped_obj["type"] = 'IMAGE'
  mapped_obj["subtype"] = s3key.split('.')[-1]
  mapped_obj["sizeBytes"] = str(media_obj.get('size',0))
  mapped_obj["height"] = ''
  mapped_obj["width"] = ''
  mapped_obj["locationStatus"] = 'SUCCESS'
  mapped_obj["metadataStatus"] = 'SUCCESS'
  mapped_obj["locationError"] = ''
  mapped_obj["metadataError"] = ''
  mapped_obj["mimeType"] = 'migrated'



def get_mapped_catalogue_object(mapped_obj, media_obj, bucket, s3key):
  mapped_obj["key"] = s3key
  mapped_obj["bucket"] = bucket
  mapped_obj["type"] = 'CATALOGUE'
  mapped_obj["subtype"] = ''
  mapped_obj["sizeBytes"] = ''
  mapped_obj["numPages"] = media_obj.get('contentParts', 0)
  mapped_obj["locationStatus"] = "SUCCESS"
  mapped_obj["metadataStatus"] = "SUCCESS"
  mapped_obj["locationError"] = ''
  mapped_obj["metadataError"] = ''
  mapped_obj["mimeType"] = ''


def get_subtitle_media(mapped_obj, media_obj, bucket, s3key):
  
  mapped_obj["bucket"] = bucket
  mapped_obj["key"] = s3key
  mapped_obj["type"] = 'DOCUMENT'
  mapped_obj["subtype"] = 'VTT'
  mapped_obj["sizeBytes"] = '0'
  mapped_obj["width"] = ''
  mapped_obj["numPages"] = '1'
  mapped_obj["locationStatus"] = 'SUCCESS'
  mapped_obj["metadataStatus"] = 'SUCCESS'
  mapped_obj["locationError"] = ''
  mapped_obj["metadataError"] = ''
  mapped_obj["mimeType"] = '.vtt'
  return mapped_obj

def get_transcription_media(mapped_obj, media_obj, bucket, s3key):
  
  mapped_obj["bucket"] =bucket
  mapped_obj["key"] = s3key
  mapped_obj["type"] = 'DOCUMENT'
  mapped_obj["subtype"] = 'JSON'
  mapped_obj["sizeBytes"] = '0'
  mapped_obj["width"] = ''
  mapped_obj["numPages"] = '1'
  mapped_obj["locationStatus"] = 'SUCCESS'
  mapped_obj["metadataStatus"] = 'SUCCESS'
  mapped_obj["locationError"] = ''
  mapped_obj["metadataError"] = ''
  mapped_obj["mimeType"] = '.json'
  return mapped_obj


def get_document_objects(mapped_obj, cb_obj, cdn):

  mapped_list = []
  obj_id, media_obj = cb_obj['id'], cb_obj['ce']

  if 'original_path' in media_obj and media_obj['original_path'] is not None:
    original_path = media_obj['original_path']
    mobj = copy.deepcopy(mapped_obj)
    mapped_list.append(get_mapped_document_obj(mobj, media_obj, region_bucket[cdn], original_path))

    if media_obj.get('uuid') or media_obj['docProcessor'] == 'BOX' or media_obj['docProcessor'] == 'HYBRID':
      if media_obj['docProcessor'] == 'HTML_PDF_LAMBDA':
        pdf = get_mapped_document_obj(copy.deepcopy(mapped_obj), media_obj, region_bucket[cdn], original_path + "/doc.pdf")
        thumb = get_mapped_image_object(copy.deepcopy(mapped_obj), media_obj, croco_bucket[cdn], original_path + "/imagified/out_1.png")
        mapped_list.append(pdf)
        mapped_list.append(thumb)

      if media_obj['docProcessor'] == 'CROCODOC' or media_obj['docProcessor'] == None:
        uuid = media_obj.get('uuid','')
        pdf = get_mapped_document_obj(copy.deepcopy(mapped_obj), media_obj,croco_bucket[cdn], uuid + "/doc.pdf")
        thumb = get_mapped_image_object(copy.deepcopy(mapped_obj), media_obj, croco_bucket[cdn],  uuid + "/images/thumbnail-master-0.png")
        mapped_list.append(pdf)
        mapped_list.append(thumb)

      else:
        pdf = get_mapped_document_obj(copy.deepcopy(mapped_obj), media_obj, box_bucket[cdn], obj_id + "/doc.pdf")
        thumb = get_mapped_image_object(copy.deepcopy(mapped_obj), media_obj, box_bucket[cdn],  obj_id + "/imagified/out_1.png")
        mapped_list.append(pdf)
        mapped_list.append(thumb)

    # if media_obj.get('imagifiedStatus') == "IMAGIFIED_SUCCESS":
    #   content_parts = media_obj.get('contentParts', 0)
    #   catalogues_objs = get_mapped_catalogue_object(copy.deepcopy(mapped_obj), media_obj, region_bucket[cdn], original_path+'/imagified/out')
    #   mapped_list.extend(catalogues_objs)

  return mapped_list



def get_mapped_media_objects(cb_obj):
  media_obj = cb_obj['ce']
  cId = media_obj.get('companyId', "")
  comp_obj = companySettings.get(f'{cId}.settings')
  if comp_obj is None:
    return []

  type = media_obj['type']
  user_id = media_obj.get('uploadedById', '_default_migrated')

  cdnId = comp_obj['cdnId']
  orgId = comp_obj["orgId"]
  company_id = media_obj['companyId']

  mapped_obj = {}
  mapped_obj = get_media_object(type)
  mapped_obj["orgId"] = orgId
  mapped_obj["companyId"] = company_id
  mapped_obj["tenantId"] = orgId
  mapped_obj["authorizer"] = user_id
  mapped_obj["globalContextId"] = mapped_obj["id"]
  mapped_obj["source"] = 'content-engine-migration'

  mapped_obj["region"] = region_cdn[cdnId]
  mapped_obj['name'] = media_obj.get('title', '')

  if docs[type] == 'AUDIO':
    mapped_objs = get_audio_objects(mapped_obj, cb_obj, cdnId)
  else:
    mapped_objs = get_document_objects(mapped_obj, cb_obj, cdnId)

  return mapped_objs


def get_create_requests(cb_obj, user_id, tenant_id):
  docs=[]
  req_context = RequestContext(user_id=user_id, tenant_id=tenant_id)
  mapped_docs = get_mapped_media_objects(cb_obj)
  for doc in mapped_docs:
    docs.append(getDoc(doc['id'], json.dumps(doc)))
  return CreateDocsRequest(request_context= req_context, collection_id=collections['infra_media'], doc=docs)



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


async def migrate_company(comp_id, medias):

  try:

    create_requests_list = []
    for media_obj in medias:
      type = media_obj['ce'].get('type', 0)
      if type not in mediaTypes:
        continue
      ce_obj = media_obj['ce']
      cId = comp_id
      comp_obj = companySettings.get(f'{cId}.settings')
      if comp_obj is None:
        continue
      user_id = ce_obj.get('uploadedById', '_default_migrated')
      orgId = comp_obj["orgId"]
      company_id = ce_obj['companyId']

      create_requests = get_create_requests(media_obj, user_id, orgId)
      create_requests_list.append(create_requests)


    l = len(create_requests_list)
    for i in range((l//10)+1):
      batch_list = create_requests_list[i*10:(i+1)*10]
      if len(batch_list) > 0:
        modifyDocsRequest = ModifyDocsRequest(request_context=RequestContext(user_id=user_id, tenant_id=orgId),
                                              create_docs_request=batch_list)
        resp = stub.ModifyDocs(modifyDocsRequest)
        if resp['status']:
          processed_companies.add(media_obj['id'])
        else:
          raise Exception("create media failed")

  except Exception as e:
    failed_companies.add(comp_id)
    return




async def companies_migration(type='QA'):
  with open(get_file_name("", type)) as f:
    csv_reader = csv.reader(f)
    # for row in csv_reader:
    #   migrate_company(row[0], type)
    await asyncio.gather(*[migrate_company(cmp[0], type) for cmp in csv_reader])




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
  path = get_dir("downloaded")
  files = glob.glob(path+'/*.csv')
  comp = []
  for fl in files:
    f = fl.split('/')[-1]
    cmp = f.split('.')[0].strip('media_to_migrate_')
    comp.append(cmp)
  return comp




def load_company_settings():
  global companySettings

  with open(f'company_settings.csv') as f:
    csv_reader = csv.reader(f)
    for row in csv_reader:
      id, obj = row[0], json.loads(row[1])
      companySettings[id] = obj




def read_company_settings_from_db():
  company_settings = {}
  companies_by_types = N1QLRequest(_N1QLQuery('SELECT META().id,* FROM ce WHERE type=$1', 3), cb)
  for comp_obj in companies_by_types:
    company_settings[comp_obj['id']] = comp_obj['ce']

  with open('company_settings.csv', 'w') as f:
    csv_writer = csv.writer(f)
    for cId, cObj in company_settings.items():
      csv_writer.writerow([cId, json.dumps(cObj)])



async def migrate_media_by_company(companies_list=[]):
  global processed_companies
  companies_list = processed_companies
  tasks=[]
  path = get_dir('downloaded')

  for comp_id in companies_list:
    medias = []
    file = os.path.join(path, f'media_to_migrate_{comp_id}.csv')
    with open(file) as f:
      csv_reader = csv.reader(f)
      for row in csv_reader:
        media_obj = json.loads(row[0])
        medias.append(media_obj)
    # migrate_company(comp_id, medias)
    tasks.append(migrate_company(comp_id, medias))

  await asyncio.gather(*tasks)



failed_db_reads = []
cnt=0

async def read_media_by_company(comp_id):
  global processed_companies
  media_records = []
  global cnt
  try:
    medias = N1QLRequest(
      _N1QLQuery('SELECT META().id, * FROM ce WHERE companyId=$1', comp_id), cb)
    for media_obj in medias:
      if media_obj['ce'].get('type') not in mediaTypes:
        continue
      cid = media_obj['ce']['companyId']
      media_records.append([json.dumps(media_obj)])

  except Exception as e:
    failed_db_reads.extend(comp_id)
    return

  path = get_dir('downloaded')
  file = os.path.join(path, f'media_to_migrate_{comp_id}.csv')
  with open(file, 'w') as f:
    csv_writer = csv.writer(f)
    csv_writer.writerows(media_records)

  processed_companies.append(comp_id)



def get_companies_by_type(comp_type = 'ALL'):
  if comp_type=='ALL':
    return [comp.strip('.settings') for comp in companySettings.keys()]

  if comp_type not in companyTypes:
    return []

  companies = [comp.strip('.settings') for comp in companySettings if companySettings[comp]['companyType']==comp_type]
  return companies




async def read_batch_to_migrate_from_db(comp_list):

  await asyncio.gather(*[read_media_by_company(cmp) for cmp in comp_list])

  # for comp in companies_list[:20]:
  #   read_media_by_company(comp)




# def documents_data_migration():
#   for comp_type in companyTypes:
#     data_mig(comp_type)
#   # res = await asyncio.gather(*[data_mig(comp_type) for comp_type in companyTypes])


async def main():


  read_company_settings_from_db()
  load_company_settings()


  comp_list = get_companies_by_type()
  comp_list = comp_list[:2]
  task = asyncio.create_task(read_batch_to_migrate_from_db(comp_list))
  await task

  # load_companies_to_process()
  mig_task = asyncio.create_task(migrate_media_by_company())
  await mig_task


  # read_processed_companies()


  # write_processed_migrations(comp_type)
  # write_failed_migrations(comp_type)


if __name__ == '__main__':
  # main()
  asyncio.run(main())
