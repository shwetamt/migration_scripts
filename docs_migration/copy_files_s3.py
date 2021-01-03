import boto3
import os, csv

s3_resource = boto3.resource('s3')
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
    with open(f'{path}/object_paths_{comp_id}.csv') as f1, open(f'{failed_path}/failed_object_paths_{comp_id}.csv', 'a') as f2:
        path_reader= csv.reader(f1)
        failed_writer = csv.writer(f2)
        for row in path_reader:
            obj = {'from_bucket': row[0], 'from_file': row[1], 'to_bucket': row[2], 'to_file': row[3]}
            try:
                copy_to_bucket(obj)
                print(f'success - {obj}')
            except Exception as e:
                failed_writer.writerow(row)
                print(f'Exception while copying file for - {obj} - {e}')
    print(f'Copied files for company - {comp_id}')



def main():
    global sub_dir
    sub_dir = 'Testing0'
    comp_list = load_companies_for_mapping()
    for comp in comp_list:
        copy_paths_by_company(comp)



if __name__ == '__main__':
   main()



