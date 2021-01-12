import boto3
import os, csv
import re

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


def copy_to_folder(obj, already_copied):
    print(f'copying folder for {obj}')
    try:
        response = s3.list_objects(
            Bucket=obj['from_bucket'],
            Prefix=obj['from_folder'])
        from_folder = obj['from_folder']
        to_folder = obj['to_folder']
    except Exception as e:
        print(f'Exception while accesing folder contents - {e}')
        raise Exception(f'Exception while accesing folder contents - {e}')
    objects_list = response.get('Contents', [])
    paths = []
    for s3_obj in objects_list:
        key = s3_obj['Key']
        if key in already_copied:
            continue
        root = key.split('/')
        sub = from_folder.split('/')
        suff = "/".join(root[len(sub):])
        mod_obj = {'from_bucket': obj['from_bucket'], 'from_file': key, 'to_bucket': obj['to_bucket'], 'to_file': f'{to_folder}/{suff}'}
        try:
            copy_to_bucket(mod_obj)
            paths.append([obj['from_bucket'], key, obj['to_bucket'], f'{to_folder}/{suff}'])
            print(f'sucess - {mod_obj}')
        except Exception as e:
            print(f'Exception while copying object - {mod_obj} - {e}')
    return paths


# def tt():
#     current_dir = os.getcwd()
#     s3_resource.Object('mtgame-us.mindtickle.com',
#                        '817283497610854710/1556724340655DruvaPhoenixWhiteboardPitchScoreSheetv3.xlsx').download_file(
#         f'/{current_dir}/test_boto')

def copy_to_bucket(obj):
    copy_source = {
        'Bucket': obj['from_bucket'],
        'Key': obj['from_file']
    }
    s3_resource.Object(obj['to_bucket'], obj['to_file']).copy(copy_source)



def copy_paths_by_company(comp_id):
    path = get_dir("object_paths", sub_dir)
    if not os.path.exists(f'{path}/object_paths_{comp_id}.csv'):
        print(f'Exception while opening file for {comp_id}')
        return
    print(f'Start copying files for company - {comp_id}')
    failed_path = get_dir("failed_object_paths", sub_dir)
    copied_path = get_dir("copied_object_paths", sub_dir)
    already_copied = get_already_copied_paths(comp_id)
    with open(f'{path}/object_paths_{comp_id}.csv') as f1, open(f'{failed_path}/failed_object_paths_{comp_id}.csv', 'w') as f2, open(f'{copied_path}/copied_object_paths_{comp_id}.csv', 'a') as f3:
        path_reader= csv.reader(f1)
        failed_writer = csv.writer(f2)
        copied_writer = csv.writer(f3)
        for row in path_reader:
            if row[1] in already_copied:
                continue
            try:
                if row[1].endswith('{image_num}.png'):
                    from_fol = row[1].split('/')
                    from_fol = "/".join(from_fol[:len(from_fol)-1])
                    to_fol = row[3].split('/')
                    to_fol = "/".join(from_fol[:len(to_fol) - 1])
                    obj = {'from_bucket': row[0], 'from_folder': from_fol, 'to_bucket': row[2], 'to_folder': to_fol}
                    paths = copy_to_folder(obj, already_copied)
                    copied_writer.writerows(paths)
                else:
                    obj = {'from_bucket': row[0], 'from_file': row[1], 'to_bucket': row[2], 'to_file': row[3]}
                    copy_to_bucket(obj)
                    copied_writer.writerow(row)

                print(f'success - {obj}')
            except Exception as e:
                failed_writer.writerow(row+[e])
                print(f'Exception while copying file/files for - {obj} - {e}')
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
#    row='mtgame-cdn.mindtickle.com,1261175967008605974/1589875870994major.pdf/imagified/out_{image_num}.png,mt-picasso-asia-singapore,1261175961909629773/1261175967008605974/CATALOGUE21011114383323644344/out_{image_num}.png'
#    row=row.split(',')
#    if row[1].endswith('{image_num}.png'):
#        obj = {'from_bucket': row[0], 'from_folder': row[1].rstrip('/out_{image_num}.png'), 'to_bucket': row[2],
#               'to_folder': row[3].rstrip('/out_{image_num}.png')}
#        paths = copy_to_folder(obj, set())


