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
        'ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.0.7_smooth_compressed.tif'),
    #esa_1992: https://esa-worldcover.org/en/data-access
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
    'peat': (
        '+3',  # this adds "3" to whatever the landcover code is
        'https://archive.researchdata.leeds.ac.uk/251/'),
}

WORKSPACE_DIR = 'workspace_combine_marine_terrestrial'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')


def _download_task(task_graph, url, target_path):
    """Wrapper for taskgraph call."""
    return
    download_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(url, target_path),
        kwargs={'skip_if_target_exists': True},
        target_path_list=[target_path],
        task_id=f'downloading {os.path.basename(url)}')
    return download_task


def main():
    """Entry point."""
    os.makedirs(WORKSPACE_DIR, exist_ok=True)
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
            target_path_list=[converted_lulc_path],
            task_id=f'convert {lulc_path}')

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
