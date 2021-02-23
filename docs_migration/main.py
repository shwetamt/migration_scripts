from docs_mapping_from_infra_to_picasso import map_medias_from_infra_to_picasso
from docs_mapping_to_infra import map_medias_to_infra
from update_cb_object import enable_picasso, update_cb_object
from docs_data_read_from_cb import read_data_for_migration
import asyncio
from copy_files_s3 import copy_object_paths


def main(comp_list, dir):
    # update_cb_object(comp_list[0], None, 2)
    # enable_picasso(comp_list, dir, 0)
    read_data_for_migration(comp_list, dir)
    # asyncio.run(map_medias_to_infra(comp_list, dir))
    # asyncio.run(map_medias_from_infra_to_picasso(comp_list, dir))
    copy_object_paths(comp_list, dir)
    # enable_picasso(comp_list, dir, 1)


if __name__=='__main__':
    dir = 'pikachu'
    comp_list = ['1361642392611509413']
    main(comp_list, dir)