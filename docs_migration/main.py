from docs_mapping_from_infra_to_picasso import map_medias_from_infra_to_picasso
from docs_mapping_to_infra import map_medias_to_infra
from update_cb_object import enable_picasso
from copy_files_s3 import copy_object_paths
from docs_data_read_from_cb import read_data_for_migration
import asyncio


def main(comp_list, dir):
    read_data_for_migration(comp_list, dir)
    asyncio.run(map_medias_to_infra(comp_list, dir))
    copy_object_paths(comp_list, dir)
    asyncio.run(map_medias_from_infra_to_picasso(comp_list, dir))
    enable_picasso(comp_list, dir)


if __name__=='__main__':
    dir = 'Final_test'
    comp_list = ['1261175967008605974']
    main(comp_list, dir)