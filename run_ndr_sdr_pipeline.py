"""Entry point to manage data and run pipeline."""
from datetime import datetime
import collections
import glob
import gzip
import itertools
import logging
import multiprocessing
import os
import shutil
import sys
import threading
import time

from inspring import sdr_c_factor
from inspring import ndr_mfd_plus
from ecoshard import geoprocessing
from ecoshard import taskgraph
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import ecoshard
import requests


gdal.SetCacheMax(2**26)
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    filename='sdrndrlog.txt')
logging.getLogger('ecoshard.taskgraph').setLevel(logging.INFO)
logging.getLogger('ecoshard.ecoshard').setLevel(logging.INFO)
logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
logging.getLogger('ecoshard.geoprocessing.geoprocessing').setLevel(
    logging.ERROR)
logging.getLogger('ecoshard.geoprocessing.routing.routing').setLevel(
    logging.WARNING)
logging.getLogger('ecoshard.geoprocessing.geoprocessing_core').setLevel(
    logging.ERROR)
logging.getLogger('inspring.sdr_c_factor').setLevel(logging.WARNING)
logging.getLogger('inspring.ndr_mfd_plus').setLevel(logging.WARNING)

LOGGER = logging.getLogger(__name__)

WORKSPACE_DIR = 'workspace'
SDR_WORKSPACE_DIR = os.path.join(WORKSPACE_DIR, 'sdr_workspace')
NDR_WORKSPACE_DIR = os.path.join(WORKSPACE_DIR, 'ndr_workspace')
WATERSHED_SUBSET_TOKEN_PATH = os.path.join(
    WORKSPACE_DIR, 'watershed_partition.token')

# how many jobs to hold back before calling stitcher
N_TO_BUFFER_STITCH = 10

TARGET_PIXEL_SIZE_M = 300  # pixel size in m when operating on projected data
GLOBAL_PIXEL_SIZE_DEG = 10/3600  # 10s resolution
GLOBAL_BB = [-179.9, -60, 179.9, 60]

# These SDR constants are what we used as the defaults in a previous project
THRESHOLD_FLOW_ACCUMULATION = 1000
L_CAP = 122
K_PARAM = 2
SDR_MAX = 0.8
IC_0_PARAM = 0.5

DEM_KEY = 'dem'
EROSIVITY_KEY = 'erosivity'
ERODIBILITY_KEY = 'erodibility'
ESA_LULC_KEY = 'esa_lulc'
SCENARIO_1_LULC_KEY = 'scenario_1_lulc'
SCENARIO_1_V2_LULC_KEY = 'scenario_1_v2_lulc'
SDR_BIOPHYSICAL_TABLE_KEY = 'sdr_biophysical_table'
WATERSHEDS_KEY = 'watersheds'
WAVES_KEY = 'waves'
SDR_BIOPHYSICAL_TABLE_LUCODE_KEY = 'ID'
NDR_BIOPHYSICAL_TABLE_LUCODE_KEY = 'Value'

RUNOFF_PROXY_KEY = 'Precipitation'
FERTILZER_KEY = 'Fertilizer'
NDR_BIOPHYSICAL_TABLE_KEY = 'ndr_biophysical_table'
LULC_MODVCFTREE1KM_KEY = 'esa_modVCFTree1km'
MODVCFTREE1KM_BIOPHYSICAL_TABLE_KEY = 'tree1km_biophysical_table'
MODVCFTREE1KM_BIOPHYSICAL_TABLE_LUCODE_ID = 'ID'

SKIP_TASK_SET = {
    'sdr au_bas_15s_beta_176_'
}

SCENARIO_2_V3_LULC_KEY = 'Sc2v3_Griscom_CookPatton2050_smithpnv'
SCENARIO_2_V4_LULC_KEY = 'Sc2v4_Griscom_CookPatton2035_smithpnv'
SCENARIO_1_V3_LULC_KEY = 'Sc1v3_restoration_pnv0_5_on_ESA2020mVCF'
SCENARIO_1_V4_LULC_KEY = 'Sc1v4_restoration_pnv0_001_on_ESA2020mVCF'
NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY = 'new_esa_biophysical_121621'
NEW_ESA_BIOPHYSICAL_121621_TABLE_LUCODE_VALUE = 'ID'
ESAMOD2_LULC_KEY = 'esamod2'
SC1V5RENATO_GT_0_5_LULC_KEY = 'sc1v5renato_gt_0_5'
SC1V6RENATO_GT_0_001_LULC_KEY = 'sc1v6renato_gt_0_001'
SC2V5GRISCOM2035_LULC_KEY = 'sc2v5griscom2035'
SC2V6GRISCOM2050_LULC_KEY = 'sc2v6griscom2050'
SC3V1PNVNOAG_LULC_KEY = 'sc3v1pnvnoag'
SC3V2PNVALL_LULC_KEY = 'sc3v2pnvall'
LULC_SC1_KEY = 'lulc_sc1'
LULC_SC2_KEY = 'lulc_sc2'
LULC_SC3_KEY = 'lulc_sc3'
FERTILIZER_CURRENT_KEY = 'fertilizer_current'
FERTILIZER_INTENSIFIED_KEY = 'fertilizer_intensified'
FERTILIZER_2050_KEY = 'fertilizer_2050'
HE60PR50_PRECIP_KEY = 'he60pr50'
NLCD_BIOPHYSICAL_TABLE_KEY = 'nlcd_biophysical'
NLCD_COTTON_TO_83_KEY = 'nlcd2016_cotton_to_83'
BASE_NLCD_KEY = 'nlcd2016'
NLCD_LUCODE = 'lulc'

ECOSHARD_MAP = {
    ESAMOD2_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/ESAmodVCFv2_md5_05407ed305c24604eb5a38551cddb031.tif',
    SC1V5RENATO_GT_0_5_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc1v5_md5_85604d25eb189f3566712feb506a8b9f.tif',
    SC1V6RENATO_GT_0_001_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc1v6_md5_c3539eae022a1bf588142bc363edf5a3.tif',
    SC2V5GRISCOM2035_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc2v5_md5_a3ce41871b255adcd6e1c65abfb1ddd0.tif',
    SC2V6GRISCOM2050_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc2v6_md5_dc75e27f0cb49a84e082a7467bd11214.tif',
    SC3V1PNVNOAG_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc3v1_PNVnoag_md5_c07865b995f9ab2236b8df0378f9206f.tif',
    SC3V2PNVALL_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc3v2_PNVallhab_md5_419ab9f579d10d9abb03635c5fdbc7ca.tif',

    LULC_SC1_KEY: 'https://storage.googleapis.com/ecoshard-root/cbd/scenarios/ESA_2015_mod_IIS_md5_c5063ced9f1c75ebdf6da2ac006afecd.tif',
    LULC_SC2_KEY: 'https://storage.googleapis.com/ecoshard-root/cbd/scenarios/reclassified_NCIallag_bare_to_sparse_md5_6683283f691fef0507c6909e9786be1a.tif',
    LULC_SC3_KEY: 'https://storage.googleapis.com/ecoshard-root/cbd/scenarios/reclassified_PNV_smith_bare_to_sparse_md5_18d50e06130765b80064c824601f7c47.tif',

    BASE_NLCD_KEY: ('https://storage.googleapis.com/ecoshard-root/gee_export/nlcd2016_compressed_md5_f372b.tif', 0),
    NLCD_COTTON_TO_83_KEY: ('https://storage.googleapis.com/ecoshard-root/cbd/scenarios/cotton_to_83_nlcd2016_md5_f265f5.tif', 0),

    FERTILIZER_CURRENT_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Nrates_NCIcurrentRevQ_add_smithpnv_background_md5_0cdf5cd1c3ba6e1032fcac63174fa8e1.tif',
    FERTILIZER_INTENSIFIED_KEY: 'https://storage.googleapis.com/ecoshard-root/cbd/scenarios/finaltotalNfertratesirrigatedRevQ_add_background_md5_b763d688a87360d37868d6a0fbd6b68a.tif',
    FERTILIZER_2050_KEY: 'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/ag_load_scenarios/ssp3_2050_ag_load_md5_9fab631dfdae22d12cd92bb1983f9ef1.tif',

    LULC_MODVCFTREE1KM_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020_modVCFTree1km_md5_1cef3d5ad126b8bb34deb19d9ffc7d46.tif',
    ESA_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/esa_lulc_smoothed/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1_md5_2ed6285e6f8ec1e7e0b75309cc6d6f9f.tif',
    #MODVCFTREE1KM_BIOPHYSICAL_TABLE_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/new_esa_biophysical_121621_md5_b0c83182473b6c2203012385187490e3.csv',
    #SCENARIO_1_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/restoration_pnv0.0001_on_ESA2020_clip_md5_93d43b6124c73cb5dc21698ea5f9c8f4.tif',
    #SCENARIO_1_V2_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/restoration_pnv0.0001_on_ESA2020_v2_md5_47613f8e4d340c92b2c481cc8080cc9d.tif',
    SCENARIO_2_V3_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc2v3_Griscom_CookPatton2050_smithpnv_md5_82c2f863d49f5a25c0b857865bfdb4b0.tif',
    SCENARIO_2_V4_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc2v4_Griscom_CookPatton2035_smithpnv_md5_ffde2403583e30d7df4d16a0687d71fe.tif',
    SCENARIO_1_V3_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc1v3_restoration_pnv0.5_on_ESA2020mVCF_md5_403f35b2a8b9b917090703e291f6bc0c.tif',
    SCENARIO_1_V4_LULC_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/Sc1v4_restoration_pnv0.001_on_ESA2020mVCF_md5_61a44df722532a84a77598fe2a24d46c.tif',

    NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/new_esa_biophysical_121621_md5_b0c83182473b6c2203012385187490e3.csv',

    NLCD_BIOPHYSICAL_TABLE_KEY: 'https://storage.googleapis.com/ecoshard-root/ci_global_restoration/nlcd_biophysical_md5_92b0d4c44168f7595be66daff611203f.csv',
    DEM_KEY: 'https://storage.googleapis.com/ecoshard-root/global-invest-sdr-data/global_dem_3s_md5_22d0c3809af491fa09d03002bdf09748.zip',
    EROSIVITY_KEY: 'https://storage.googleapis.com/ecoshard-root/global-invest-sdr-data/GlobalR_NoPol_compressed_md5_49734c4b1c9c94e49fffd0c39de9bf0c.tif',
    ERODIBILITY_KEY: 'https://storage.googleapis.com/ecoshard-root/pasquale/Kfac_SoilGrid1km_GloSEM_v1.1_md5_e1c74b67ad7fdaf6f69f1f722a5c7dfb.tif',
    WATERSHEDS_KEY: 'https://storage.googleapis.com/ecoshard-root/global-invest-sdr-data/watersheds_globe_HydroSHEDS_15arcseconds_md5_c6acf2762123bbd5de605358e733a304.zip',
    RUNOFF_PROXY_KEY: 'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/worldclim_2015_md5_16356b3770460a390de7e761a27dbfa1.tif',
    HE60PR50_PRECIP_KEY: 'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/precip_scenarios/he60pr50_md5_829fbd47b8fefb064ae837cbe4d9f4be.tif',
    FERTILZER_KEY: 'https://storage.googleapis.com/nci-ecoshards/scenarios050420/NCI_Ext_RevB_add_backgroundN_md5_e4a9cc537cd0092d346e4287e7bd4c36.tif',
    'Global polygon': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_global_polygon_simplified_geometries_md5_653118dde775057e24de52542b01eaee.gpkg',
    'Buffered shore': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/buffered_global_shore_5km_md5_a68e1049c1c03673add014cd29b7b368.gpkg',
    'Shore grid': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/shore_grid_md5_07aea173cf373474c096f1d5e3463c2f.gpkg',
    WAVES_KEY: 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/wave_watch_iii_md5_c8bb1ce4739e0a27ee608303c217ab5b.gpkg.gz',
    'Coastal DEM': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/global_dem_md5_22c5c09ac4c4c722c844ab331b34996c.tif',
    'SLR': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/MSL_Map_MERGED_Global_AVISO_NoGIA_Adjust_md5_3072845759841d0b2523d00fe9518fee.tif',
    'Geomorphology': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/geomorphology_md5_e65eff55840e7a80cfcb11fdad2d02d7.gpkg',
    'Coastal habitat: reef': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_reef_wgs84_compressed_md5_96d95cc4f2c5348394eccff9e8b84e6b.tif',
    'Coastal habitat: mangrove': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_mangrove_md5_0ec85cb51dab3c9ec3215783268111cc.tif',
    'Coastal habitat: seagrass': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_seagrass_md5_a9cc6d922d2e74a14f74b4107c94a0d6.tif',
    'Coastal habitat: saltmarsh': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/cv_layers/ipbes-cv_saltmarsh_md5_203d8600fd4b6df91f53f66f2a011bcd.tif',
    'Pollination-dependent yield': 'https://storage.googleapis.com/critical-natural-capital-ecoshards/monfreda_2008_yield_poll_dep_ppl_fed_5min.tif',
    'Population': 'https://storage.googleapis.com/ecoshard-root/population/lspop2019_compressed_md5_d0bf03bd0a2378196327bbe6e898b70c.tif',
    'Friction surface': 'https://storage.googleapis.com/ecoshard-root/critical_natural_capital/friction_surface_2015_v1.0-002_md5_166d17746f5dd49cfb2653d721c2267c.tif',
    'World borders': 'https://storage.googleapis.com/ecoshard-root/critical_natural_capital/TM_WORLD_BORDERS-0.3_simplified_md5_47f2059be8d4016072aa6abe77762021.gpkg',
    #'Habitat mask ESA': '(need to make from LULC above)',
    #'Habitat mask Scenario1': '(need to make from LULC above)',
    #'Coastal population': '(need to make from population above and this mask: https://storage.googleapis.com/ecoshard-root/ipbes-cv/total_pop_masked_by_10m_md5_ef02b7ee48fa100f877e3a1671564be2.tif)',
    #'Coastal habitat masks ESA': '(will be outputs of CV)',
    #'Coastal habitat masks Scenario 1': '(will be outputs of CV)',
    }


def _flatten_dir(working_dir):
    """Move all files in subdirectory to `working_dir`."""
    all_files = []
    # itertools lets us skip the first iteration (current dir)
    for root, _dirs, files in itertools.islice(os.walk(working_dir), 1, None):
        for filename in files:
            all_files.append(os.path.join(root, filename))
    for filename in all_files:
        shutil.move(filename, working_dir)


def _unpack_and_vrt_tiles(
        zip_path, unpack_dir, target_nodata, target_vrt_path):
    """Unzip multi-file of tiles and create VRT.

    Args:
        zip_path (str): path to zip file of tiles
        unpack_dir (str): path to directory to unpack tiles
        target_vrt_path (str): desired target path for VRT.

    Returns:
        None
    """
    if not os.path.exists(target_vrt_path):
        shutil.unpack_archive(zip_path, unpack_dir)
        _flatten_dir(unpack_dir)
        base_raster_path_list = glob.glob(os.path.join(unpack_dir, '*.tif'))
        vrt_options = gdal.BuildVRTOptions(VRTNodata=target_nodata)
        gdal.BuildVRT(
            target_vrt_path, base_raster_path_list, options=vrt_options)
        target_dem = gdal.OpenEx(target_vrt_path, gdal.OF_RASTER)
        if target_dem is None:
            raise RuntimeError(
                f"didn't make VRT at {target_vrt_path} on: {zip_path}")


def _download_and_validate(url, target_path):
    """Download an ecoshard and validate its hash."""
    ecoshard.download_url(url, target_path)
    if not ecoshard.validate(target_path):
        raise ValueError(f'{target_path} did not validate on its hash')


def _download_and_set_nodata(url, nodata, target_path):
    """Download and set nodata value if needed."""
    ecoshard.download_url(url, target_path)
    if nodata is not None:
        raster = gdal.OpenEx(target_path, gdal.GA_Update)
        band = raster.GetRasterBand(1)
        band.SetNoDataValue(nodata)
        band = None
        raster = None


def fetch_data(ecoshard_map, data_dir):
    """Download data in `ecoshard_map` and replace urls with targets.

    Any values that are not urls are kept and a warning is logged.

    Args:
        ecoshard_map (dict): key/value pairs where if value is a url that
            file is downloaded and verified against its hash.
        data_dir (str): path to a directory to store downloaded data.

    Returns:
        dict of {value: filepath} map where `filepath` is the path to the
            downloaded file stored in `data_dir`. If the original value was
            not a url it is copied as-is.
    """
    task_graph = taskgraph.TaskGraph(
        data_dir, multiprocessing.cpu_count(), parallel_mode='thread',
        taskgraph_name='fetch data')
    data_map = {}
    for key, value in ecoshard_map.items():
        if isinstance(value, tuple):
            url, nodata = value
        else:
            url = value
        if url.startswith('http'):
            target_path = os.path.join(data_dir, os.path.basename(url))
            data_map[key] = target_path
            if os.path.exists(target_path):
                LOGGER.info(f'{target_path} exists, so skipping download')
                continue
            response = requests.head(url)
            if response:
                target_path = os.path.join(data_dir, os.path.basename(url))
                if not os.path.exists(target_path):
                    task_graph.add_task(
                        func=_download_and_set_nodata,
                        args=(url, nodata, target_path),
                        target_path_list=[target_path],
                        task_name=f'download {url}')
            else:
                raise ValueError(f'{key}: {url} does not refer to a url')
        else:
            if not os.path.exists(url):
                raise ValueError(
                    f'expected an existing file at {url} but not found')
            data_map[key] = url
    LOGGER.info('waiting for downloads to complete')
    task_graph.close()
    task_graph.join()
    task_graph = None
    return data_map


def _unpack_archive(archive_path, dest_dir):
    """Unpack archive to dest_dir."""
    if archive_path.endswith('.gz'):
        with gzip.open(archive_path, 'r') as f_in:
            dest_path = os.path.join(
                dest_dir, os.path.basename(os.path.splitext(archive_path)[0]))
            with open(dest_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        shutil.unpack_archive(archive_path, dest_dir)


def fetch_and_unpack_data(task_graph):
    """Fetch & unpack data subroutine."""
    data_dir = os.path.join(WORKSPACE_DIR, 'data')
    LOGGER.info('downloading data')
    fetch_task = task_graph.add_task(
        func=fetch_data,
        args=(ECOSHARD_MAP, data_dir),
        store_result=True,
        transient_run=True,
        task_name='download ecoshards')
    file_map = fetch_task.get()
    LOGGER.info('downloaded data')
    dem_dir = os.path.join(data_dir, DEM_KEY)
    dem_vrt_path = os.path.join(dem_dir, 'dem.vrt')
    LOGGER.info('unpack dem')
    _ = task_graph.add_task(
        func=_unpack_and_vrt_tiles,
        args=(file_map[DEM_KEY], dem_dir, -9999, dem_vrt_path),
        target_path_list=[dem_vrt_path],
        task_name=f'unpack {file_map[DEM_KEY]}')
    file_map[DEM_KEY] = dem_vrt_path
    for compressed_id in [WAVES_KEY, WATERSHEDS_KEY]:
        _ = task_graph.add_task(
            func=_unpack_archive,
            args=(file_map[compressed_id], data_dir),
            task_name=f'decompress {file_map[compressed_id]}')
        file_map[compressed_id] = data_dir
    LOGGER.debug('wait for unpack')
    task_graph.join()

    # just need the base directory for watersheds
    file_map[WATERSHEDS_KEY] = os.path.join(
        file_map[WATERSHEDS_KEY], 'watersheds_globe_HydroSHEDS_15arcseconds')

    # only need to lose the .gz on the waves
    file_map[WAVES_KEY] = os.path.splitext(file_map[WAVES_KEY])

    return file_map


def _batch_into_watershed_subsets(
        watershed_root_dir, degree_separation, done_token_path,
        watershed_subset=None):
    """Construct geospatially adjacent subsets.

    Breaks watersheds up into geospatially similar watersheds and limits
    the upper size to no more than specified area. This allows for a
    computationally efficient batch to run on a large contiguous area in
    parallel while avoiding batching watersheds that are too small.

    Args:
        watershed_root_dir (str): path to watershed .shp files.
        degree_separation (int): a blocksize number of degrees to coalasce
            watershed subsets into.
        done_token_path (str): path to file to write when function is
            complete, indicates for batching that the task is complete.
        watershed_subset (dict): if not None, keys are watershed basefile
            names and values are FIDs to select. If present the simulation
            only constructs batches from these watershed/fids, otherwise
            all watersheds are run.

    Returns:
        list of (job_id, watershed.gpkg) tuples where the job_id is a
        unique identifier for that subwatershed set and watershed.gpkg is
        a subset of the original global watershed files.

    """
    # ensures we don't have more than 1000 watersheds per job
    task_graph = taskgraph.TaskGraph(
        watershed_root_dir, multiprocessing.cpu_count(), 10,
        taskgraph_name='batch watersheds')
    watershed_path_area_list = []
    job_id_set = set()
    for watershed_path in glob.glob(
            os.path.join(watershed_root_dir, '*.shp')):
        LOGGER.debug(f'scheduling {os.path.basename(watershed_path)}')
        subbatch_job_index_map = collections.defaultdict(int)
        # lambda describes the FIDs to process per job, the list of lat/lng
        # bounding boxes for each FID, and the total degree area of the job
        watershed_fid_index = collections.defaultdict(
            lambda: [list(), list(), 0])
        watershed_basename = os.path.splitext(
            os.path.basename(watershed_path))[0]
        watershed_ids = None
        watershed_vector = gdal.OpenEx(watershed_path, gdal.OF_VECTOR)
        watershed_layer = watershed_vector.GetLayer()

        if watershed_subset:
            if watershed_basename not in watershed_subset:
                continue
            else:
                # just grab the subset
                watershed_ids = watershed_subset[watershed_basename]
                watershed_layer = [
                    watershed_layer.GetFeature(fid) for fid in watershed_ids]

        # watershed layer is either the layer or a list of features
        for watershed_feature in watershed_layer:
            fid = watershed_feature.GetFID()
            watershed_geom = watershed_feature.GetGeometryRef()
            watershed_centroid = watershed_geom.Centroid()
            epsg = geoprocessing.get_utm_zone(
                watershed_centroid.GetX(), watershed_centroid.GetY())
            if watershed_geom.Area() > 1 or watershed_ids:
                # one degree grids or immediates get special treatment
                job_id = (f'{watershed_basename}_{fid}', epsg)
                watershed_fid_index[job_id][0] = [fid]
            else:
                # clamp into degree_separation squares
                x, y = [
                    int(v//degree_separation)*degree_separation for v in (
                        watershed_centroid.GetX(), watershed_centroid.GetY())]
                base_job_id = f'{watershed_basename}_{x}_{y}'
                # keep the epsg in the string because the centroid might lie
                # on a different boundary
                job_id = (f'''{base_job_id}_{
                    subbatch_job_index_map[base_job_id]}_{epsg}''', epsg)
                if len(watershed_fid_index[job_id][0]) > 1000:
                    subbatch_job_index_map[base_job_id] += 1
                    job_id = (f'''{base_job_id}_{
                        subbatch_job_index_map[base_job_id]}_{epsg}''', epsg)
                watershed_fid_index[job_id][0].append(fid)
            watershed_envelope = watershed_geom.GetEnvelope()
            watershed_bb = [watershed_envelope[i] for i in [0, 2, 1, 3]]
            if (watershed_bb[0] < GLOBAL_BB[0] or
                    watershed_bb[2] > GLOBAL_BB[2] or
                    watershed_bb[1] > GLOBAL_BB[3] or
                    watershed_bb[3] < GLOBAL_BB[1]):
                LOGGER.warn(
                    f'{watershed_bb} is on a dangerous boundary so dropping')
                watershed_fid_index[job_id][0].pop()
                continue
            watershed_fid_index[job_id][1].append(watershed_bb)
            watershed_fid_index[job_id][2] += watershed_geom.Area()

        watershed_geom = None
        watershed_feature = None

        watershed_subset_dir = os.path.join(
            watershed_root_dir, 'watershed_subsets')
        os.makedirs(watershed_subset_dir, exist_ok=True)

        for (job_id, epsg), (fid_list, watershed_envelope_list, area) in \
                sorted(
                    watershed_fid_index.items(), key=lambda x: x[1][-1],
                    reverse=True):
            if job_id in job_id_set:
                raise ValueError(f'{job_id} already processed')
            if len(watershed_envelope_list) < 3 and area < 0.0001:
                # it's too small to process
                continue
            job_id_set.add(job_id)

            watershed_subset_path = os.path.join(
                watershed_subset_dir, f'{job_id}_a{area:.3f}.gpkg')
            if not os.path.exists(watershed_subset_path):
                task_graph.add_task(
                    func=_create_fid_subset,
                    args=(
                        watershed_path, fid_list, epsg, watershed_subset_path),
                    target_path_list=[watershed_subset_path],
                    task_name=job_id)
            watershed_path_area_list.append(
                (area, watershed_subset_path))

        watershed_layer = None
        watershed_vector = None

    task_graph.join()
    task_graph.close()
    task_graph = None

    # create a global sorted watershed path list so it's sorted by area overall
    # not just by region per area
    with open(done_token_path, 'w') as token_file:
        token_file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    sorted_watershed_path_list = [
        path for area, path in sorted(watershed_path_area_list, reverse=True)]
    return sorted_watershed_path_list


def _create_fid_subset(
        base_vector_path, fid_list, target_epsg, target_vector_path):
    """Create subset of vector that matches fid list, projected into epsg."""
    vector = gdal.OpenEx(base_vector_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(target_epsg)
    layer.SetAttributeFilter(
        f'"FID" in ('
        f'{", ".join([str(v) for v in fid_list])})')
    feature_count = layer.GetFeatureCount()
    gpkg_driver = ogr.GetDriverByName('gpkg')
    unprojected_vector_path = '%s_wgs84%s' % os.path.splitext(
        target_vector_path)
    subset_vector = gpkg_driver.CreateDataSource(unprojected_vector_path)
    subset_vector.CopyLayer(
        layer, os.path.basename(os.path.splitext(target_vector_path)[0]))
    geoprocessing.reproject_vector(
        unprojected_vector_path, srs.ExportToWkt(), target_vector_path,
        driver_name='gpkg', copy_fields=False)
    subset_vector = None
    layer = None
    vector = None
    gpkg_driver.DeleteDataSource(unprojected_vector_path)
    target_vector = gdal.OpenEx(target_vector_path, gdal.OF_VECTOR)
    target_layer = target_vector.GetLayer()
    if feature_count != target_layer.GetFeatureCount():
        raise ValueError(
            f'expected {feature_count} in {target_vector_path} but got '
            f'{target_layer.GetFeatureCount()}')


def _run_sdr(
        task_graph,
        workspace_dir,
        watershed_path_list,
        dem_path,
        erosivity_path,
        erodibility_path,
        lulc_path,
        target_pixel_size,
        biophysical_table_path,
        biophysical_table_lucode_field,
        threshold_flow_accumulation,
        l_cap,
        k_param,
        sdr_max,
        ic_0_param,
        target_stitch_raster_map,
        keep_intermediate_files=False,
        c_factor_path=None,
        result_suffix=None,
        ):
    """Run SDR component of the pipeline.

    This function will iterate through the watershed subset list, run the SDR
    model on those subwatershed regions, and stitch those data back into a
    global raster.

    Args:
        workspace_dir (str): path to directory to do all work
        watershed_path_list (list): list of watershed vector files to
            operate on locally. The base filenames are used as the workspace
            directory path.
        dem_path (str): path to global DEM raster
        erosivity_path (str): path to global erosivity raster
        erodibility_path (str): path to erodability raster
        lulc_path (str): path to landcover raster
        target_pixel_size (float): target projected pixel unit size
        biophysical_table_lucode_field (str): name of the lucode field in
            the biophysical table column
        threshold_flow_accumulation (float): flow accumulation threshold
            to use to calculate streams.
        l_cap (float): upper limit to the L factor
        k_param (float): k parameter in SDR model
        sdr_max (float): max SDR value
        ic_0_param (float): IC0 constant in SDR model
        target_stitch_raster_map (dict): maps the local path of an output
            raster of this model to an existing global raster to stich into.
        keep_intermediate_files (bool): if True, the intermediate watershed
            workspace created underneath `workspace_dir` is deleted.
        c_factor_path (str): optional, path to c factor that's used for lucodes
            that use the raster
        result_suffix (str): optional, prepended to the global stitch results.

    Returns:
        None.
    """
    # create intersecting bounding box of input data
    global_wgs84_bb = _calculate_intersecting_bounding_box(
        [dem_path, erosivity_path, erodibility_path, lulc_path])

    # create global stitch rasters and start workers
    stitch_raster_queue_map = {}
    stitch_worker_list = []
    multiprocessing_manager = multiprocessing.Manager()
    signal_done_queue = multiprocessing_manager.Queue()
    for local_result_path, global_stitch_raster_path in \
            target_stitch_raster_map.items():
        if result_suffix is not None:
            global_stitch_raster_path = (
                f'%s_{result_suffix}%s' % os.path.splitext(
                    global_stitch_raster_path))
            local_result_path = (
                f'%s_{result_suffix}%s' % os.path.splitext(
                    local_result_path))
        if not os.path.exists(global_stitch_raster_path):
            LOGGER.info(f'creating {global_stitch_raster_path}')
            driver = gdal.GetDriverByName('GTiff')
            n_cols = int((global_wgs84_bb[2]-global_wgs84_bb[0])/GLOBAL_PIXEL_SIZE_DEG)
            n_rows = int((global_wgs84_bb[3]-global_wgs84_bb[1])/GLOBAL_PIXEL_SIZE_DEG)
            LOGGER.info(f'**** creating raster of size {n_cols} by {n_rows}')
            target_raster = driver.Create(
                global_stitch_raster_path,
                n_cols, n_rows, 1,
                gdal.GDT_Float32,
                options=(
                    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
                    'SPARSE_OK=TRUE', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))
            wgs84_srs = osr.SpatialReference()
            wgs84_srs.ImportFromEPSG(4326)
            target_raster.SetProjection(wgs84_srs.ExportToWkt())
            target_raster.SetGeoTransform(
                [global_wgs84_bb[0], GLOBAL_PIXEL_SIZE_DEG, 0,
                 global_wgs84_bb[3], 0, -GLOBAL_PIXEL_SIZE_DEG])
            target_band = target_raster.GetRasterBand(1)
            target_band.SetNoDataValue(-9999)
            target_raster = None
        stitch_queue = multiprocessing_manager.Queue(N_TO_BUFFER_STITCH*2)
        stitch_thread = threading.Thread(
            target=stitch_worker,
            args=(
                stitch_queue, global_stitch_raster_path,
                len(watershed_path_list),
                signal_done_queue))
        stitch_thread.start()
        stitch_raster_queue_map[local_result_path] = stitch_queue
        stitch_worker_list.append(stitch_thread)

    clean_workspace_worker = threading.Thread(
        target=_clean_workspace_worker,
        args=(len(target_stitch_raster_map), signal_done_queue,
              keep_intermediate_files))
    clean_workspace_worker.daemon = True
    clean_workspace_worker.start()

    # Iterate through each watershed subset and run SDR
    # stitch the results of whatever outputs to whatever global output raster.
    for index, watershed_path in enumerate(watershed_path_list):
        local_workspace_dir = os.path.join(
            workspace_dir, os.path.splitext(
                os.path.basename(watershed_path))[0])
        task_name = f'sdr {os.path.basename(local_workspace_dir)}'
        if any([sub in task_name for sub in SKIP_TASK_SET]):
            continue
        task_graph.add_task(
            func=_execute_sdr_job,
            args=(
                global_wgs84_bb, watershed_path, local_workspace_dir,
                dem_path, erosivity_path, erodibility_path, lulc_path,
                biophysical_table_path, threshold_flow_accumulation, k_param,
                sdr_max, ic_0_param, target_pixel_size,
                biophysical_table_lucode_field, stitch_raster_queue_map,
                result_suffix),
            transient_run=False,
            priority=-index,  # priority in insert order
            task_name=task_name)

    LOGGER.info('wait for SDR jobs to complete')
    task_graph.join()
    for local_result_path, stitch_queue in stitch_raster_queue_map.items():
        stitch_queue.put(None)
    LOGGER.info('all done with SDR, waiting for stitcher to terminate')
    for stitch_thread in stitch_worker_list:
        stitch_thread.join()
    LOGGER.info(
        'all done with stitching, waiting for workspace worker to terminate')
    signal_done_queue.put(None)
    clean_workspace_worker.join()

    LOGGER.info('all done with SDR -- stitcher terminated')


def _execute_sdr_job(
        global_wgs84_bb, watersheds_path, local_workspace_dir, dem_path,
        erosivity_path, erodibility_path, lulc_path, biophysical_table_path,
        threshold_flow_accumulation, k_param, sdr_max, ic_0_param,
        target_pixel_size, biophysical_table_lucode_field,
        stitch_raster_queue_map, result_suffix):
    """Worker to execute sdr and send signals to stitcher.

    Args:
        global_wgs84_bb (list): bounding box to limit run to, if watersheds do
            not fit, then skip
        watersheds_path (str): path to watershed to run model over
        local_workspace_dir (str): path to local directory

        SDR arguments:
            dem_path
            erosivity_path
            erodibility_path
            lulc_path
            biophysical_table_path
            threshold_flow_accumulation
            k_param
            sdr_max
            ic_0_param
            target_pixel_size
            biophysical_table_lucode_field
            result_suffix

        stitch_raster_queue_map (dict): map of local result path to
            the stitch queue to signal when job is done.

    Returns:
        None.
    """
    if not _watersheds_intersect(global_wgs84_bb, watersheds_path):
        LOGGER.debug(f'{watersheds_path} does not overlap {global_wgs84_bb}')
        for local_result_path, stitch_queue in stitch_raster_queue_map.items():
            # indicate skipping
            stitch_queue.put((None, 1))

        return

    local_sdr_taskgraph = taskgraph.TaskGraph(local_workspace_dir, -1)
    dem_pixel_size = geoprocessing.get_raster_info(dem_path)['pixel_size']
    base_raster_path_list = [
        dem_path, erosivity_path, erodibility_path, lulc_path]
    resample_method_list = ['bilinear', 'bilinear', 'bilinear', 'mode']

    clipped_data_dir = os.path.join(local_workspace_dir, 'data')
    os.makedirs(clipped_data_dir, exist_ok=True)
    watershed_info = geoprocessing.get_vector_info(watersheds_path)
    target_projection_wkt = watershed_info['projection_wkt']
    watershed_bb = watershed_info['bounding_box']
    lat_lng_bb = geoprocessing.transform_bounding_box(
        watershed_bb, target_projection_wkt, osr.SRS_WKT_WGS84_LAT_LONG)

    warped_raster_path_list = [
        os.path.join(clipped_data_dir, os.path.basename(path))
        for path in base_raster_path_list]

    # TODO: figure out bounding box then individually warp so we don't
    # re-warp stuff we already did
    _warp_raster_stack(
        local_sdr_taskgraph, base_raster_path_list, warped_raster_path_list,
        resample_method_list, dem_pixel_size, target_pixel_size,
        lat_lng_bb, osr.SRS_WKT_WGS84_LAT_LONG, watersheds_path)
    local_sdr_taskgraph.join()

    # clip to lat/lng bounding boxes
    args = {
        'workspace_dir': local_workspace_dir,
        'dem_path': warped_raster_path_list[0],
        'erosivity_path': warped_raster_path_list[1],
        'erodibility_path': warped_raster_path_list[2],
        'lulc_path': warped_raster_path_list[3],
        'prealigned': True,
        'watersheds_path': watersheds_path,
        'biophysical_table_path': biophysical_table_path,
        'threshold_flow_accumulation': threshold_flow_accumulation,
        'k_param': k_param,
        'sdr_max': sdr_max,
        'ic_0_param': ic_0_param,
        'results_suffix': result_suffix,
        'biophysical_table_lucode_field': biophysical_table_lucode_field,
        'single_outlet': geoprocessing.get_vector_info(
            watersheds_path)['feature_count'] == 1,
        'prealigned': True,
        'reuse_dem': True,
    }
    sdr_c_factor.execute(args)
    for local_result_path, stitch_queue in stitch_raster_queue_map.items():
        stitch_queue.put(
            (os.path.join(args['workspace_dir'], local_result_path), 1))


def _execute_ndr_job(
        global_wgs84_bb, watersheds_path, local_workspace_dir, dem_path,
        lulc_path,
        runoff_proxy_path, fertilizer_path, biophysical_table_path,
        threshold_flow_accumulation, k_param, target_pixel_size,
        biophysical_table_lucode_field, stitch_raster_queue_map,
        result_suffix):
    """Execute NDR for watershed and push to stitch raster.

        Args:
            global_wgs84_bb (list): global bounding box to test watershed
                overlap with

        args['workspace_dir'] (string):  path to current workspace
        args['dem_path'] (string): path to digital elevation map raster
        args['lulc_path'] (string): a path to landcover map raster
        args['runoff_proxy_path'] (string): a path to a runoff proxy raster
        args['watersheds_path'] (string): path to the watershed shapefile
        args['biophysical_table_path'] (string): path to csv table on disk
            containing nutrient retention values.

            Must contain the following headers:

            'load_n', 'eff_n', 'crit_len_n'

        args['results_suffix'] (string): (optional) a text field to append to
            all output files
        rgs['fertilizer_path'] (string): path to raster to use for fertlizer
            rates when biophysical table uses a 'use raster' value for the
            biophysical table field.
        args['threshold_flow_accumulation']: a number representing the flow
            accumulation in terms of upstream pixels.
        args['k_param'] (number): The Borselli k parameter. This is a
            calibration parameter that determines the shape of the
            relationship between hydrologic connectivity.
        args['target_pixel_size'] (2-tuple): optional, requested target pixel
            size in local projection coordinate system. If not provided the
            pixel size is the smallest of all the input rasters.
        args['target_projection_wkt'] (str): optional, if provided the
            model is run in this target projection. Otherwise runs in the DEM
            projection.
        args['single_outlet'] (str): if True only one drain is modeled, either
            a large sink or the lowest pixel on the edge of the dem.
        result_suffix (str): string to append to NDR files.
    """
    if not _watersheds_intersect(global_wgs84_bb, watersheds_path):
        for local_result_path, stitch_queue in stitch_raster_queue_map.items():
            # indicate skipping
            stitch_queue.put((None, 1))
        return

    local_ndr_taskgraph = taskgraph.TaskGraph(local_workspace_dir, -1)
    dem_pixel_size = geoprocessing.get_raster_info(dem_path)['pixel_size']
    base_raster_path_list = [
        dem_path, runoff_proxy_path, lulc_path, fertilizer_path]
    resample_method_list = ['bilinear', 'bilinear', 'mode', 'bilinear']

    clipped_data_dir = os.path.join(local_workspace_dir, 'data')
    os.makedirs(clipped_data_dir, exist_ok=True)
    watershed_info = geoprocessing.get_vector_info(watersheds_path)
    target_projection_wkt = watershed_info['projection_wkt']
    watershed_bb = watershed_info['bounding_box']
    lat_lng_bb = geoprocessing.transform_bounding_box(
        watershed_bb, target_projection_wkt, osr.SRS_WKT_WGS84_LAT_LONG)

    warped_raster_path_list = [
        os.path.join(clipped_data_dir, os.path.basename(path))
        for path in base_raster_path_list]

    _warp_raster_stack(
        local_ndr_taskgraph, base_raster_path_list, warped_raster_path_list,
        resample_method_list, dem_pixel_size, target_pixel_size,
        lat_lng_bb, osr.SRS_WKT_WGS84_LAT_LONG, watersheds_path)
    local_ndr_taskgraph.join()

    args = {
        'workspace_dir': local_workspace_dir,
        'dem_path': warped_raster_path_list[0],
        'runoff_proxy_path': warped_raster_path_list[1],
        'lulc_path': warped_raster_path_list[2],
        'fertilizer_path': warped_raster_path_list[3],
        'watersheds_path': watersheds_path,
        'biophysical_table_path': biophysical_table_path,
        'threshold_flow_accumulation': threshold_flow_accumulation,
        'k_param': k_param,
        'target_pixel_size': (target_pixel_size, -target_pixel_size),
        'target_projection_wkt': target_projection_wkt,
        'single_outlet': geoprocessing.get_vector_info(
            watersheds_path)['feature_count'] == 1,
        'biophyisical_lucode_fieldname': biophysical_table_lucode_field,
        'crit_len_n': 150.0,
        'prealigned': True,
        'reuse_dem': True,
        'results_suffix': result_suffix,
    }
    ndr_mfd_plus.execute(args)
    for local_result_path, stitch_queue in stitch_raster_queue_map.items():
        stitch_queue.put(
            (os.path.join(args['workspace_dir'], local_result_path), 1))


def _clean_workspace_worker(
        expected_signal_count, stitch_done_queue, keep_intermediate_files):
    """Removes workspaces when completed.

    Args:
        expected_signal_count (int): the number of times to be notified
            of a done path before it should be deleted.
        stitch_done_queue (queue): will contain directory paths with the
            same directory path appearing `expected_signal_count` times,
            the directory will be removed. Recieving `None` will terminate
            the process.
        keep_intermediate_files (bool): keep intermediate files if true

    Returns:
        None
    """
    try:
        count_dict = collections.defaultdict(int)
        while True:
            dir_path = stitch_done_queue.get()
            if dir_path is None:
                LOGGER.info('recieved None, quitting clean_workspace_worker')
                return
            count_dict[dir_path] += 1
            if count_dict[dir_path] == expected_signal_count:
                LOGGER.info(
                    f'removing {dir_path} after {count_dict[dir_path]} '
                    f'signals')
                if not keep_intermediate_files:
                    shutil.rmtree(dir_path)
                del count_dict[dir_path]
    except Exception:
        LOGGER.exception('error on clean_workspace_worker')


def stitch_worker(
        rasters_to_stitch_queue, target_stitch_raster_path, n_expected,
        signal_done_queue):
    """Update the database with completed work.

    Args:
        rasters_to_stitch_queue (queue): queue that recieves paths to
            rasters to stitch into target_stitch_raster_path.
        target_stitch_raster_path (str): path to an existing raster to stitch
            into.
        n_expected (int): number of expected stitch signals
        signal_done_queue (queue): as each job is complete the directory path
            to the raster will be passed in to eventually remove.


    Return:
        ``None``
    """
    try:
        processed_so_far = 0
        n_buffered = 0
        start_time = time.time()
        stitch_buffer_list = []
        LOGGER.info(f'started stitch worker for {target_stitch_raster_path}')
        while True:
            payload = rasters_to_stitch_queue.get()
            if payload is not None:
                if payload[0] is None:  # means skip this raster
                    processed_so_far += 1
                    continue
                stitch_buffer_list.append(payload)

            if len(stitch_buffer_list) > N_TO_BUFFER_STITCH or payload is None:
                LOGGER.info(
                    f'about to stitch {n_buffered} into '
                    f'{target_stitch_raster_path}')
                geoprocessing.stitch_rasters(
                    stitch_buffer_list, ['near']*len(stitch_buffer_list),
                    (target_stitch_raster_path, 1),
                    area_weight_m2_to_wgs84=True,
                    overlap_algorithm='replace')
                #  _ is the band number
                for stitch_path, _ in stitch_buffer_list:
                    signal_done_queue.put(os.path.dirname(stitch_path))
                stitch_buffer_list = []

            if payload is None:
                LOGGER.info(f'all done sitching {target_stitch_raster_path}')
                return

            processed_so_far += 1
            jobs_per_sec = processed_so_far / (time.time() - start_time)
            remaining_time_s = (
                n_expected / jobs_per_sec)
            remaining_time_h = int(remaining_time_s // 3600)
            remaining_time_s -= remaining_time_h * 3600
            remaining_time_m = int(remaining_time_s // 60)
            remaining_time_s -= remaining_time_m * 60
            LOGGER.info(
                f'remaining jobs to process for {target_stitch_raster_path}: '
                f'{n_expected-processed_so_far} - '
                f'processed so far {processed_so_far} - '
                f'process/sec: {jobs_per_sec:.1f}s - '
                f'time left: {remaining_time_h}:'
                f'{remaining_time_m:02d}:{remaining_time_s:04.1f}')
    except Exception:
        LOGGER.exception(
            f'error on stitch worker for {target_stitch_raster_path}')
        raise


def _run_ndr(
        task_graph,
        workspace_dir,
        watershed_path_list,
        dem_path,
        runoff_proxy_path,
        fertilizer_path,
        lulc_path,
        target_pixel_size,
        biophysical_table_path,
        biophysical_table_lucode_field,
        threshold_flow_accumulation,
        k_param,
        target_stitch_raster_map,
        keep_intermediate_files=False,
        result_suffix=None,):

    # create intersecting bounding box of input data
    global_wgs84_bb = _calculate_intersecting_bounding_box(
        [dem_path, runoff_proxy_path, fertilizer_path, lulc_path])

    stitch_raster_queue_map = {}
    stitch_worker_list = []
    multiprocessing_manager = multiprocessing.Manager()
    signal_done_queue = multiprocessing_manager.Queue()
    for local_result_path, global_stitch_raster_path in \
            target_stitch_raster_map.items():
        if result_suffix is not None:
            global_stitch_raster_path = (
                f'%s_{result_suffix}%s' % os.path.splitext(
                    global_stitch_raster_path))
            local_result_path = (
                f'%s_{result_suffix}%s' % os.path.splitext(
                    local_result_path))
        if not os.path.exists(global_stitch_raster_path):
            LOGGER.info(f'creating {global_stitch_raster_path}')
            driver = gdal.GetDriverByName('GTiff')
            n_cols = int((GLOBAL_BB[2]-GLOBAL_BB[0])/GLOBAL_PIXEL_SIZE_DEG)
            n_rows = int((GLOBAL_BB[3]-GLOBAL_BB[1])/GLOBAL_PIXEL_SIZE_DEG)
            LOGGER.info(f'**** creating raster of size {n_cols} by {n_rows}')
            target_raster = driver.Create(
                global_stitch_raster_path,
                n_cols, n_rows, 1,
                gdal.GDT_Float32,
                options=(
                    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
                    'SPARSE_OK=TRUE', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))
            wgs84_srs = osr.SpatialReference()
            wgs84_srs.ImportFromEPSG(4326)
            target_raster.SetProjection(wgs84_srs.ExportToWkt())
            target_raster.SetGeoTransform(
                [GLOBAL_BB[0], GLOBAL_PIXEL_SIZE_DEG, 0,
                 GLOBAL_BB[3], 0, -GLOBAL_PIXEL_SIZE_DEG])
            target_band = target_raster.GetRasterBand(1)
            target_band.SetNoDataValue(-9999)
            target_raster = None
        stitch_queue = multiprocessing_manager.Queue(N_TO_BUFFER_STITCH*2)
        stitch_thread = threading.Thread(
            target=stitch_worker,
            args=(
                stitch_queue, global_stitch_raster_path,
                len(watershed_path_list),
                signal_done_queue))
        stitch_thread.start()
        stitch_raster_queue_map[local_result_path] = stitch_queue
        stitch_worker_list.append(stitch_thread)

    clean_workspace_worker = threading.Thread(
        target=_clean_workspace_worker,
        args=(
            len(target_stitch_raster_map), signal_done_queue,
            keep_intermediate_files))
    clean_workspace_worker.daemon = True
    clean_workspace_worker.start()

    # Iterate through each watershed subset and run ndr
    # stitch the results of whatever outputs to whatever global output raster.
    for index, watershed_path in enumerate(watershed_path_list):
        local_workspace_dir = os.path.join(
            workspace_dir, os.path.splitext(
                os.path.basename(watershed_path))[0])
        task_graph.add_task(
            func=_execute_ndr_job,
            args=(
                global_wgs84_bb, watershed_path, local_workspace_dir, dem_path,
                lulc_path, runoff_proxy_path, fertilizer_path,
                biophysical_table_path,
                threshold_flow_accumulation, k_param, target_pixel_size,
                biophysical_table_lucode_field, stitch_raster_queue_map,
                result_suffix),
            transient_run=False,
            priority=-index,  # priority in insert order
            task_name=f'ndr {os.path.basename(local_workspace_dir)}')

    LOGGER.info('wait for ndr jobs to complete')
    task_graph.join()
    for local_result_path, stitch_queue in stitch_raster_queue_map.items():
        stitch_queue.put(None)
    LOGGER.info('all done with ndr, waiting for stitcher to terminate')
    for stitch_thread in stitch_worker_list:
        stitch_thread.join()
    LOGGER.info(
        'all done with stitching, waiting for workspace worker to terminate')
    signal_done_queue.put(None)
    clean_workspace_worker.join()

    LOGGER.info('all done with ndr -- stitcher terminated')


def main():
    """Entry point."""
    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, multiprocessing.cpu_count(), 15.0,
        parallel_mode='process', taskgraph_name='run pipeline main')
    data_map = fetch_and_unpack_data(task_graph)

    watershed_subset = {
        #'af_bas_15s_beta': [19039, 23576, 18994],
        #'au_bas_15s_beta': [125804],
        #'as_bas_15s_beta': [218032],
        'af_bas_15s_beta': [78138],
        }
    watershed_subset = None

    # make sure taskgraph doesn't re-run just because the file was opened
    watershed_subset_task = task_graph.add_task(
        func=_batch_into_watershed_subsets,
        args=(
            data_map[WATERSHEDS_KEY], 4, WATERSHED_SUBSET_TOKEN_PATH,
            watershed_subset),
        target_path_list=[WATERSHED_SUBSET_TOKEN_PATH],
        store_result=True,
        task_name='watershed subset batch')
    watershed_subset_list = watershed_subset_task.get()

    task_graph.join()

    sdr_target_stitch_raster_map = {
        'sed_export.tif': os.path.join(
            WORKSPACE_DIR, 'global_sed_export.tif'),
        'sed_retention.tif': os.path.join(
            WORKSPACE_DIR, 'global_sed_retention.tif'),
        'sed_deposition.tif': os.path.join(
            WORKSPACE_DIR, 'global_sed_deposition.tif'),
        'usle.tif': os.path.join(
            WORKSPACE_DIR, 'global_usle.tif'),
    }

    ndr_target_stitch_raster_map = {
        'n_export.tif': os.path.join(
            WORKSPACE_DIR, 'global_n_export.tif'),
        'n_retention.tif': os.path.join(
            WORKSPACE_DIR, 'global_n_retention.tif'),
        os.path.join('intermediate_outputs', 'modified_load_n.tif'): os.path.join(
            WORKSPACE_DIR, 'global_modified_load_n.tif'),
    }

    run_sdr = True
    run_ndr = True
    keep_intermediate_files = True
    dem_key = os.path.basename(os.path.splitext(data_map[DEM_KEY])[0])
    sdr_run_set = set()
    for lulc_key, biophysical_table_key, lucode, fert_key in [
            #(ESAMOD2_LULC_KEY, None),
            #(SC1V5RENATO_GT_0_5_LULC_KEY, None),
            #(SC1V6RENATO_GT_0_001_LULC_KEY, None),
            #(SC2V5GRISCOM2035_LULC_KEY, None),
            #(SC2V6GRISCOM2050_LULC_KEY, None),
            #(SC3V1PNVNOAG_LULC_KEY, None),
            #(SC3V2PNVALL_LULC_KEY, None),
            #(LULC_SC1_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_LUCODE_VALUE, FERTILIZER_CURRENT_KEY),
            #(LULC_SC2_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_LUCODE_VALUE, FERTILIZER_CURRENT_KEY),
            #(LULC_SC3_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_LUCODE_VALUE, FERTILIZER_CURRENT_KEY),
            #(LULC_SC1_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_LUCODE_VALUE, FERTILIZER_INTENSIFIED_KEY),
            #(LULC_SC2_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_LUCODE_VALUE, FERTILIZER_INTENSIFIED_KEY),
            #(LULC_SC1_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_LUCODE_VALUE, FERTILIZER_2050_KEY),
            #(LULC_SC2_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_KEY, NEW_ESA_BIOPHYSICAL_121621_TABLE_LUCODE_VALUE, FERTILIZER_2050_KEY),
            (NLCD_COTTON_TO_83_KEY, NLCD_BIOPHYSICAL_TABLE_KEY, NLCD_LUCODE, FERTILIZER_CURRENT_KEY),
            (BASE_NLCD_KEY, NLCD_BIOPHYSICAL_TABLE_KEY, NLCD_LUCODE, FERTILIZER_CURRENT_KEY),
            ]:

        if run_sdr:
            sdr_workspace_dir = os.path.join(SDR_WORKSPACE_DIR, dem_key)
            # SDR doesn't have fert scenarios
            if lulc_key in sdr_run_set:
                continue
            sdr_run_set.add(lulc_key)
            _run_sdr(
                task_graph=task_graph,
                workspace_dir=sdr_workspace_dir,
                watershed_path_list=watershed_subset_list,
                dem_path=data_map[DEM_KEY],
                erosivity_path=data_map[EROSIVITY_KEY],
                erodibility_path=data_map[ERODIBILITY_KEY],
                lulc_path=data_map[lulc_key],
                target_pixel_size=TARGET_PIXEL_SIZE_M,
                biophysical_table_path=data_map[biophysical_table_key],
                biophysical_table_lucode_field=lucode,
                threshold_flow_accumulation=THRESHOLD_FLOW_ACCUMULATION,
                l_cap=L_CAP,
                k_param=K_PARAM,
                sdr_max=SDR_MAX,
                ic_0_param=IC_0_PARAM,
                target_stitch_raster_map=sdr_target_stitch_raster_map,
                keep_intermediate_files=keep_intermediate_files,
                result_suffix=lulc_key,
                )

        if run_ndr:
            ndr_workspace_dir = os.path.join(NDR_WORKSPACE_DIR, dem_key)
            if fert_key is None:
                fert_key = FERTILZER_KEY
            result_suffix = f'{lulc_key}_{fert_key}'
            _run_ndr(
                task_graph=task_graph,
                workspace_dir=ndr_workspace_dir,
                runoff_proxy_path=data_map[HE60PR50_PRECIP_KEY],
                fertilizer_path=data_map[fert_key],
                biophysical_table_path=data_map[biophysical_table_key],
                biophysical_table_lucode_field=lucode,
                watershed_path_list=watershed_subset_list,
                dem_path=data_map[DEM_KEY],
                lulc_path=data_map[lulc_key],
                target_pixel_size=TARGET_PIXEL_SIZE_M,
                threshold_flow_accumulation=THRESHOLD_FLOW_ACCUMULATION,
                k_param=K_PARAM,
                target_stitch_raster_map=ndr_target_stitch_raster_map,
                keep_intermediate_files=keep_intermediate_files,
                result_suffix=result_suffix,
                )


def _warp_raster_stack(
        task_graph, base_raster_path_list, warped_raster_path_list,
        resample_method_list, clip_pixel_size, target_pixel_size,
        clip_bounding_box, clip_projection_wkt, watershed_clip_vector_path):
    """Do an align of all the rasters but use a taskgraph to do it.

    Arguments are same as geoprocessing.align_and_resize_raster_stack.
    """
    for raster_path, warped_raster_path, resample_method in zip(
            base_raster_path_list, warped_raster_path_list,
            resample_method_list):
        working_dir = os.path.dirname(warped_raster_path)
        # first clip to clip projection
        clipped_raster_path = '%s_clipped%s' % os.path.splitext(
            warped_raster_path)
        task_graph.add_task(
            func=geoprocessing.warp_raster,
            args=(
                raster_path, clip_pixel_size, clipped_raster_path,
                resample_method),
            kwargs={
                'target_bb': clip_bounding_box,
                'target_projection_wkt': clip_projection_wkt,
                'working_dir': working_dir
                },
            target_path_list=[clipped_raster_path],
            task_name=f'clipping {clipped_raster_path}')

        # second, warp and mask to vector
        watershed_projection_wkt = geoprocessing.get_vector_info(
            watershed_clip_vector_path)['projection_wkt']

        vector_mask_options = {'mask_vector_path': watershed_clip_vector_path}
        task_graph.add_task(
            func=geoprocessing.warp_raster,
            args=(
                clipped_raster_path, (target_pixel_size, -target_pixel_size),
                warped_raster_path, resample_method,),
            kwargs={
                'target_projection_wkt': watershed_projection_wkt,
                'vector_mask_options': vector_mask_options,
                'working_dir': working_dir,
                },
            target_path_list=[warped_raster_path],
            task_name=f'warping {warped_raster_path}')


def _calculate_intersecting_bounding_box(raster_path_list):
    # create intersecting bounding box of input data
    raster_info_list = [
        geoprocessing.get_raster_info(raster_path)
        for raster_path in raster_path_list]
    raster_bounding_box_list = [
        geoprocessing.transform_bounding_box(
            info['bounding_box'],
            info['projection_wkt'],
            osr.SRS_WKT_WGS84_LAT_LONG)
        for info in raster_info_list]

    target_bounding_box = geoprocessing.merge_bounding_box_list(
            raster_bounding_box_list, 'intersection')
    LOGGER.info(f'calculated target_bounding_box: {target_bounding_box}')
    return target_bounding_box


def _watersheds_intersect(wgs84_bb, watersheds_path):
    """True if watersheds intersect the wgs84 bounding box."""
    watershed_info = geoprocessing.get_vector_info(watersheds_path)
    watershed_wgs84_bb = geoprocessing.transform_bounding_box(
        watershed_info['bounding_box'],
        watershed_info['projection_wkt'],
        osr.SRS_WKT_WGS84_LAT_LONG)
    try:
        _ = geoprocessing.merge_bounding_box_list(
            [wgs84_bb, watershed_wgs84_bb], 'intersection')
        LOGGER.info(f'{watersheds_path} intersects {wgs84_bb} with {watershed_wgs84_bb}')
        return True
    except ValueError:
        LOGGER.warn(f'{watersheds_path} does not intersect {wgs84_bb}')
        return False


if __name__ == '__main__':
    main()
