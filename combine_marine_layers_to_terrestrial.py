"""Script to add coastal habitat to global landcover.

Mangroves -> Forest
Saltmarsh -> Wetlands
Peat -> Add a "3"
"""
import logging
import os

from osgeo import gdal
from ecoshard import geoprocessing
from ecoshard import taskgraph
import ecoshard

gdal.SetCacheMax(2**26)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
logging.getLogger('ecoshard.taskgraph').setLevel(logging.WARN)
LOGGER = logging.getLogger(__name__)

LANDCOVER_DICT = {
    'esa_2020': (
        'https://storage.googleapis.com/ecoshard-root/esa_lulc_smoothed/'
        'ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif'),
    #esa_1992: https://esa-worldcover.org/en/data-access, also maybe AWS?
}

COASTAL_HAB_DICT = {
    'mangrove': (
        171,  # 170 is tree cover saline, so 171 makes it special
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/'
        'cv_layers/ipbes-cv_mangrove_md5_0ec85cb51dab3c9ec3215783268111cc.tif'),
    'saltmarsh': (
        181,  # 180 is shrub or herbacious flooded, but 181 makes it special
        'https://storage.googleapis.com/critical-natural-capital-ecoshards/'
        'cv_layers/ipbes-cv_saltmarsh_md5_203d8600fd4b6df91f53f66f2a011bcd.tif'),
    # 'peat': (
    #     '+3',  # this adds "3" to whatever the landcover code is
    #     'https://archive.researchdata.leeds.ac.uk/251/'),
}

WORKSPACE_DIR = 'workspace_combine_marine_terrestrial'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
RASTERIZED_DIR = os.path.join(WORKSPACE_DIR, 'rasterized')
ALIGNED_DIR = os.path.join(WORKSPACE_DIR, 'aligned')

for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, RASTERIZED_DIR]:
    os.makedirs(dir_path, exist_ok=True)


def _download_task(task_graph, url, target_path):
    """Wrapper for taskgraph call."""
    download_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(url, target_path),
        kwargs={'skip_if_target_exists': True},
        target_path_list=[target_path],
        task_name=f'downloading {os.path.basename(url)}')
    return download_task


def _rasterize(
        base_raster_path, vector_path, target_raster_path):
    """Rasterize vector to target that maps to base."""
    geoprocessing.new_raster_from_base(
        base_raster_path, target_raster_path, gdal.GDT_Byte, [0])
    geoprocessing.rasterize(
        vector_path, target_raster_path, burn_values=[1])


def main():
    """Entry point."""
    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)
    landcover_paths = {}
    for key, url in LANDCOVER_DICT.items():
        target_path = os.path.join(ECOSHARD_DIR, os.path.basename(url))
        landcover_paths[key] = target_path
        _ = _download_task(task_graph, url, target_path)

    hab_paths = {}
    for key, (conversion, url) in COASTAL_HAB_DICT.items():
        target_path = os.path.join(ECOSHARD_DIR, os.path.basename(url))
        hab_paths[key] = (conversion, target_path)
        _ = _download_task(task_graph, url, target_path)

    LOGGER.debug('waiting for download')
    task_graph.join()
    for lulc_key, lulc_path in landcover_paths.items():
        LOGGER.debug(f'converting {lulc_path}')
        converted_lulc_path = os.path.join(
            WORKSPACE_DIR, f'{os.path.basename(lulc_path)}')

        hab_path_list = []
        aligned_list = []
        rasterize_task_list = []
        for key, hab_path in list(hab_paths.values()):
            # if path is a vector, rasterize it and set to new path
            if hab_path.endswith('.gpkg'):
                raster_path = os.path.join(
                    RASTERIZED_DIR, lulc_key, os.path.basename(
                        os.path.splitext(hab_path)[0]))
                os.makedirs(os.dirname(raster_path), exist_ok=True)
                rasterize_task = task_graph.add_task(
                    func=_rasterize,
                    args=(lulc_path, hab_path, raster_path),
                    target_path_list=[raster_path],
                    task_name=f'rasterize {hab_path}')
                rasterize_task_list.append(rasterize_task)
                hab_path_list.append(raster_path)
            else:
                hab_path_list.append(hab_path)
            aligned_path = os.path.join(
                ALIGNED_DIR, lulc_key,
                f'aligned_{os.path.basename(hab_path_list[-1])}')
            os.makedirs(os.dirname(aligned_path), exist_ok=True)
            aligned_list.append(aligned_path)
            hab_paths[key] = aligned_path

        lulc_info = geoprocessing.get_raster_info(lulc_path)['pixel_size']
        align_task = task_graph.add_task(
            func=geoprocessing.align_and_resize_raster_stack,
            args=(
                hab_path_list, aligned_list, len(aligned_list)*['near'],
                lulc_info['pixel_size'], 'union'),
            kwargs={
                'target_projection_wkt': lulc_info['projection_wkt'],
                },
            dependent_task_list=rasterize_task_list,
            target_path_list=aligned_list,
            task_name=f'align for {lulc_key}')

        # this creates a list of tuples of the form
        # [(path0, 1), (conversion0, 'raw'), (path1, 1), ...]
        hab_conversion_list = [
            x for (conversion, path) in hab_paths.values()
            for x in ((path, 1), (conversion, 'raw'))]

        _ = task_graph.add_task(
            func=geoprocessing.raster_calculator,
            args=(
                [(lulc_path, 1)] + hab_conversion_list,
                _convert_hab_op, converted_lulc_path, gdal.GDT_Byte, 0),
            dependent_task_list=[align_task],
            target_path_list=[converted_lulc_path],
            task_name=f'convert {lulc_path}')

    task_graph.join()
    task_graph.close()


def _convert_hab_op(lulc_array, *hab_conversion_list):
    """Convert lulc to odd value if even value of hab conversion is 1."""
    result = lulc_array.copy()
    hab_iter = iter(hab_conversion_list)
    for mask_array, conversion in zip(hab_iter, hab_iter):
        if conversion.startswith('+'):
            result[mask_array] += int(conversion[1:])
        else:
            result[mask_array] = conversion
    return result


if __name__ == '__main__':
    main()
