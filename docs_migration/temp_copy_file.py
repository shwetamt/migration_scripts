import boto3
import os, csv
import re
import asyncio
import concurrent.futures
from awscli.clidriver import create_clidriver
driver = create_clidriver()
# driver.main('s3 mv s3://testing-copy-two s3://testing-copy-three --recursive'.split())


s3_resource = boto3.resource('s3')
s3 = boto3.client("s3")
#first_bucket = s3_resource.Bucket(name=first_bucket_name)

#s3_resource.Object(first_bucket_name, first_file_name).upload_file(Filename=first_file_name)
sub_dir = ''

def get_dir(prefix, subdir):
    current_dir = os.getcwd()
    dir = os.path.join(subdir, f'{prefix}_companies_media')
    if prefix == '':
        dir = sub_dir
    path = os.path.join(current_dir, dir)
    if not os.path.exists(path):
        os.makedirs(path)
    return path



def get_already_copied_paths(comp_id):
    already_copied = set()
    copied_path = get_dir("copied_object_paths", sub_dir)
    if not os.path.exists(f'{copied_path}/copied_object_paths_{comp_id}.csv'):
        return set()
    with open(f'{copied_path}/copied_object_paths_{comp_id}.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            already_copied.add(row[1])
    return already_copied




def load_companies_for_mapping():
    import glob
    global sub_dir
    print('Loading companies for processing...')
    path = get_dir("object_paths", sub_dir)
    files = glob.glob(path + '/*.csv')
    comp = []
    for fl in files:
        f = fl.split('/')[-1]
        cmp = f.split('.')[0].strip('object_paths_')
        comp.append(cmp)
    print('companies loaded...')
    return comp


def copy_to_folder(row, copied_writer, failed_writer):
    from_fol = row[1].split('/')
    from_folder = "/".join(from_fol[:len(from_fol) - 1])
    to_fol = row[3].split('/')
    to_folder = "/".join(to_fol[:len(to_fol) - 1])
    from_bucket = row[0]
    to_bucket = row[2]
    try:
        resp = driver.main(['s3','cp', f's3://{from_bucket}/{from_folder}', f's3://{to_bucket}/{to_folder}', '--recursive', '--only-show-errors'])
        if resp == 1:
            raise Exception("exception while copying")
        copied_writer.writerow(row)
        print(f'success - {row}')
        # response = s3.list_objects(
        #     Bucket=obj['from_bucket'],
        #     Prefix=obj['from_folder'])
        # from_folder = obj['from_folder']
        # to_folder = obj['to_folder']
    except Exception as e:
        failed_writer.writerow([row, e])
        print(f'Exception while copying file/files for - {row} - {e}')
    # objects_list = response.get('Contents', [])
    # paths = []
    # for s3_obj in objects_list:
    #     key = s3_obj['Key']
    #     if key in already_copied:
    #         continue
    #     root = key.split('/')
    #     sub = from_folder.split('/')
    #     suff = "/".join(root[len(sub):])
    #     mod_obj = {'from_bucket': obj['from_bucket'], 'from_file': key, 'to_bucket': obj['to_bucket'], 'to_file': f'{to_folder}/{suff}'}
    #     await copy_to_bucket(mod_obj)
    #     paths.append([obj['from_bucket'], key, obj['to_bucket'], f'{to_folder}/{suff}'])
    #     print(f'sucess - {mod_obj}')
    # return paths


# def tt():
#     current_dir = os.getcwd()
#     s3_resource.Object('mtgame-us.mindtickle.com',
#                        '817283497610854710/1556724340655DruvaPhoenixWhiteboardPitchScoreSheetv3.xlsx').download_file(
#         f'/{current_dir}/test_boto')

# async def copy_to_bucket(obj):
#     copy_source = {
#         'Bucket': obj['from_bucket'],
#         'Key': obj['from_file']
#     }
#     s3_resource.Object(obj['to_bucket'], obj['to_file']).copy(copy_source)


def copy_to_bucket(row, copied_writer, failed_writer):
    try:
        resp = driver.main(['s3','cp', f's3://{row[0]}/{row[1]}', f's3://{row[2]}/{row[3]}', '--only-show-errors'])
        if resp ==1:
            raise Exception("exception while copying")
        copied_writer.writerow(row)
        print(f'success - {row}')
    except Exception as e:
        failed_writer.writerow([row, e])
        print(f'Exception while copying file/files for - {row} - {e}')



async def process_batch(rows, copied_writer, failed_writer, already_copied):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            for row in rows:
                if row[1] in already_copied:
                    continue
                if row[1].endswith('{image_num}.png'):
                    executor.submit(copy_to_folder, row, copied_writer, failed_writer)
                    # copy_to_folder(row, copied_writer, failed_writer)
                    # tasks_folders.append(copy_to_folder(obj, already_copied))
                    # if len(paths)>0:
                    #     copied_writer.writerows(paths)
                else:
                    executor.submit(copy_to_bucket, row, copied_writer, failed_writer)
                    # copy_to_bucket(row, copied_writer, failed_writer)
                    # driver.main(f's3 cp s3://{row[0]}/{row[1]} s3://{row[2]}/{row[3]}'.split())
                    # tasks_files.append(copy_buck(row))
    except KeyboardInterrupt:
        import atexit
        atexit.unregister(concurrent.futures.thread._python_exit)
        print('Keyboard interrupt...exiting')



async def copy_paths_by_company(comp_id):
    try:
        path = get_dir("object_paths", sub_dir)
        if not os.path.exists(f'{path}/object_paths_{comp_id}.csv'):
            print(f'Exception while opening file for {comp_id}')
            return
        print(f'Start copying files for company - {comp_id}')
        failed_path = get_dir("failed_object_paths", sub_dir)
        copied_path = get_dir("copied_object_paths", sub_dir)
        already_copied = get_already_copied_paths(comp_id)
        batch_size = 0
        batch = []
        tasks = []
        with open(f'{path}/object_paths_{comp_id}.csv') as f1, open(f'{failed_path}/failed_object_paths_{comp_id}.csv',
                                                                    'w') as f2, open(f'{copied_path}/copied_object_paths_{comp_id}.csv', 'a') as f3:
            path_reader = csv.reader(f1)
            failed_writer = csv.writer(f2)
            copied_writer = csv.writer(f3)
            for row in path_reader:
                batch_size+=1
                batch.append(row)
                if batch_size == 100:
                    tasks.append(process_batch(batch, copied_writer, failed_writer, already_copied))
                    batch = []
                    batch_size = 0
            await asyncio.gather(*tasks)
    except Exception as e:
        import atexit
        atexit.unregister(concurrent.futures.thread._python_exit)
        print(f'Exception while copying file/files for - {comp_id} - {e}')
    print(f'Copied files for company - {comp_id}')




def copy_object_paths(comp_list,dir):
    print(f'Copying object paths........')
    global sub_dir
    sub_dir = dir
    # comp_list = load_companies_for_mapping()
    for comp in comp_list:
        asyncio.run(copy_paths_by_company(comp))
    print(f'Completed copying paths')



# if __name__=='__main__':
#     comp = []
#     dir = ''
#     copy_object_paths(comp, dir)


# if __name__=='__main__':
#    row='mtgame-cdn.mindtickle.com,1261175967008605974/1589875870994major.pdf/imagified/out_{image_num}.png,mt-picasso-asia-singapore,1261175961909629773/1261175967008605974/CATALOGUE21011114383323644344/out_{image_num}.png'
#    row=row.split(',')
#    if row[1].endswith('{image_num}.png'):
#        obj = {'from_bucket': row[0], 'from_folder': row[1].rstrip('/out_{image_num}.png'), 'to_bucket': row[2],
#               'to_folder': row[3].rstrip('/out_{image_num}.png')}
#        paths = copy_to_folder(obj, set())


import boto3
import os, csv
import re
import asyncio
import concurrent.futures
from awscli.clidriver import create_clidriver
import multiprocessing as mp
driver = create_clidriver()
# driver.main('s3 mv s3://testing-copy-two s3://testing-copy-three --recursive'.split())


s3_resource = boto3.resource('s3')
s3 = boto3.client("s3")
#first_bucket = s3_resource.Bucket(name=first_bucket_name)

#s3_resource.Object(first_bucket_name, first_file_name).upload_file(Filename=first_file_name)
sub_dir = ''

def get_dir(prefix, subdir):
    current_dir = os.getcwd()
    dir = os.path.join(subdir, f'{prefix}_companies_media')
    if prefix == '':
        dir = sub_dir
    path = os.path.join(current_dir, dir)
    if not os.path.exists(path):
        os.makedirs(path)
    return path



def get_already_copied_paths(comp_id):
    already_copied = set()
    copied_path = get_dir("copied_object_paths", sub_dir)
    if not os.path.exists(f'{copied_path}/copied_object_paths_{comp_id}.csv'):
        return set()
    with open(f'{copied_path}/copied_object_paths_{comp_id}.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            already_copied.add(row[1])
    return already_copied




def load_companies_for_mapping():
    import glob
    global sub_dir
    print('Loading companies for processing...')
    path = get_dir("object_paths", sub_dir)
    files = glob.glob(path + '/*.csv')
    comp = []
    for fl in files:
        f = fl.split('/')[-1]
        cmp = f.split('.')[0].strip('object_paths_')
        comp.append(cmp)
    print('companies loaded...')
    return comp


def copy_to_folder(row, q1, q2):
    from_fol = row[1].split('/')
    from_folder = "/".join(from_fol[:len(from_fol) - 1])
    to_fol = row[3].split('/')
    to_folder = "/".join(to_fol[:len(to_fol) - 1])
    from_bucket = row[0]
    to_bucket = row[2]
    try:
        resp = driver.main(['s3','cp', f's3://{from_bucket}/{from_folder}', f's3://{to_bucket}/{to_folder}', '--recursive', '--only-show-errors'])
        if resp == 1:
            raise Exception("exception while copying")
        q1.put(row)
        print(f'success - {row}')
        # response = s3.list_objects(
        #     Bucket=obj['from_bucket'],
        #     Prefix=obj['from_folder'])
        # from_folder = obj['from_folder']
        # to_folder = obj['to_folder']
    except Exception:
        q2.put(row)
    # objects_list = response.get('Contents', [])
    # paths = []
    # for s3_obj in objects_list:
    #     key = s3_obj['Key']
    #     if key in already_copied:
    #         continue
    #     root = key.split('/')
    #     sub = from_folder.split('/')
    #     suff = "/".join(root[len(sub):])
    #     mod_obj = {'from_bucket': obj['from_bucket'], 'from_file': key, 'to_bucket': obj['to_bucket'], 'to_file': f'{to_folder}/{suff}'}
    #     await copy_to_bucket(mod_obj)
    #     paths.append([obj['from_bucket'], key, obj['to_bucket'], f'{to_folder}/{suff}'])
    #     print(f'sucess - {mod_obj}')
    # return paths


# def tt():
#     current_dir = os.getcwd()
#     s3_resource.Object('mtgame-us.mindtickle.com',
#                        '817283497610854710/1556724340655DruvaPhoenixWhiteboardPitchScoreSheetv3.xlsx').download_file(
#         f'/{current_dir}/test_boto')

# async def copy_to_bucket(obj):
#     copy_source = {
#         'Bucket': obj['from_bucket'],
#         'Key': obj['from_file']
#     }
#     s3_resource.Object(obj['to_bucket'], obj['to_file']).copy(copy_source)


def copy_to_bucket(row, q1, q2):
    try:
        resp = driver.main(['s3','cp', f's3://{row[0]}/{row[1]}', f's3://{row[2]}/{row[3]}', '--recursive', '--only-show-errors'])
        if resp ==1:
            raise Exception("exception while copying")
        q1.put(row)
        print(f'success - {row}')
    except Exception:
        q2.put(row)


def success_listener(comp_id, q):
    '''listens for messages on the q, writes to file. '''
    copied_path = get_dir("copied_object_paths", sub_dir)
    with open(f'{copied_path}/copied_object_paths_{comp_id}.csv', 'a') as f:
        copied_writer = csv.writer(f)
        while 1:
            m = q.get()
            if m == 'kill':
                break
            copied_writer.writerow(m)




def failed_listener(comp_id, q):
    '''listens for messages on the q, writes to file. '''
    failed_path = get_dir("failed_object_paths", sub_dir)
    with open(f'{failed_path}/failed_object_paths_{comp_id}.csv', 'w') as f:
        failed_writer = csv.writer(f)
        while 1:
            m = q.get()
            if m == 'kill':
                break
            failed_writer.writerow(m)


def process_batch(batch, already_copied, q1, q2):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            for row in batch:
                if row[1] in already_copied:
                    continue
                if row[1].endswith('{image_num}.png'):
                    executor.submit(copy_to_folder, row, q1, q2)
                    # copy_to_folder(row, copied_writer, failed_writer)
                    # tasks_folders.append(copy_to_folder(obj, already_copied))
                    # if len(paths)>0:
                    #     copied_writer.writerows(paths)
                else:
                    executor.submit(copy_to_bucket, row, q1, q2)
                    # copy_to_bucket(row, copied_writer, failed_writer)
                    # driver.main(f's3 cp s3://{row[0]}/{row[1]} s3://{row[2]}/{row[3]}'.split())
                    # tasks_files.append(copy_buck(row))
    except KeyboardInterrupt:
        import atexit
        atexit.unregister(concurrent.futures.thread._python_exit)
        print('Keyboard interrupt...exiting')



def copy_paths_by_company(comp_id):
    try:
        path = get_dir("object_paths", sub_dir)
        if not os.path.exists(f'{path}/object_paths_{comp_id}.csv'):
            print(f'Exception while opening file for {comp_id}')
            return
        print(f'Start copying files for company - {comp_id}')
        failed_path = get_dir("failed_object_paths", sub_dir)
        copied_path = get_dir("copied_object_paths", sub_dir)
        already_copied = get_already_copied_paths(comp_id)
        tasks_files = []
        tasks_folders = []
        jobs = []
        with open(f'{path}/object_paths_{comp_id}.csv') as f1:
        # open(f'{failed_path}/failed_object_paths_{comp_id}.csv',
        #                                                             'w') as f2, open(f'{copied_path}/copied_object_paths_{comp_id}.csv', 'a') as f3:
            path_reader = csv.reader(f1)
            # failed_writer = csv.writer(f2)
            # copied_writer = csv.writer(f3)
            try:
                manager = mp.Manager()
                q1 = manager.Queue()
                q2 = manager.Queue()
                pool = mp.Pool(mp.cpu_count()+2)

                # put listener to work first
                pool.apply_async(success_listener, (comp_id,q1))
                pool.apply_async(failed_listener, (comp_id,q2))
                batch = []
                batch_size = 0
                for row in path_reader:
                    batch_size += 1
                    batch.append(row)
                    if batch_size == 100:
                        jobs.append(pool.apply_async(process_batch, (batch,already_copied, q1, q2)))
                        batch = []
                        batch_size = 0
                if batch_size>0:
                    jobs.append(pool.apply_async(process_batch, (batch, already_copied, q1, q2)))

                for job in jobs:
                    job.get()

                # now we are done, kill the listener
                q1.put('kill')
                q2.put('kill')
                pool.close()
                pool.join()
                # with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
                #     for row in path_reader:
                #         if row[1] in already_copied:
                #             continue
                #         if row[1].endswith('{image_num}.png'):
                #             executor.submit(copy_to_folder, row, copied_writer, failed_writer)
                            # copy_to_folder(row, copied_writer, failed_writer)
                            # tasks_folders.append(copy_to_folder(obj, already_copied))
                            # if len(paths)>0:
                            #     copied_writer.writerows(paths)
                        # else:
                        #     executor.submit(copy_to_bucket, row, copied_writer, failed_writer)
                            # copy_to_bucket(row, copied_writer, failed_writer)
                            # driver.main(f's3 cp s3://{row[0]}/{row[1]} s3://{row[2]}/{row[3]}'.split())
                            # tasks_files.append(copy_buck(row))
            except Exception:
                import atexit
                atexit.unregister(concurrent.futures.thread._python_exit)
                pool.close()
                pool.terminate()
    except Exception as e:
        print(f'Exception while copying file/files for - {comp_id} - {e}')
    print(f'Copied files for company - {comp_id}')




def copy_object_paths(comp_list,dir):
    print(f'Copying object paths........')
    global sub_dir
    sub_dir = dir
    # comp_list = load_companies_for_mapping()
    for comp in comp_list:
        copy_paths_by_company(comp)
    print(f'Completed copying paths')



# if __name__=='__main__':
#     comp = []
#     dir = ''
#     copy_object_paths(comp, dir)


# if __name__=='__main__':
#    row='mtgame-cdn.mindtickle.com,1261175967008605974/1589875870994major.pdf/imagified/out_{image_num}.png,mt-picasso-asia-singapore,1261175961909629773/1261175967008605974/CATALOGUE21011114383323644344/out_{image_num}.png'
#    row=row.split(',')
#    if row[1].endswith('{image_num}.png'):
#        obj = {'from_bucket': row[0], 'from_folder': row[1].rstrip('/out_{image_num}.png'), 'to_bucket': row[2],
#               'to_folder': row[3].rstrip('/out_{image_num}.png')}
#        paths = copy_to_folder(obj, set())


