import asyncio
import io
import zipfile
from asyncio import Task
from typing import Dict, List, NoReturn, Optional, Tuple

import aiohttp
import apkutils
import urllib3
from aiocache import cached
from aiohttp import ClientConnectionError, ClientError
from apksearch import generate_download_url, package_search_match
from apksearch.entities import PackageBase, PackageVariant
from apksearch.search import HEADERS
from apkutils.apkfile import BadZipFile, LargeZipFile
from loguru import logger

from mapadroid.utils import global_variables
from mapadroid.utils.apk_enums import APKArch, APKPackage, APKType
from mapadroid.utils.functions import get_version_codes

from ..db.DbWrapper import DbWrapper
from ..db.helper.MadApkAutosearchHelper import MadApkAutosearchHelper
from ..db.model import MadApkAutosearch
from ..utils.madGlobals import NoMaddevApiTokenError
from ..utils.RestHelper import RestHelper
from .abstract_apk_storage import AbstractAPKStorage
from .utils import (get_apk_info, get_supported_pogo, lookup_arch_enum,
                    lookup_package_info)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
APK_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; de; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3',
}
MAX_RETRIES: int = 3  # Number of attempts for downloading on failure


class WizardError(Exception):
    pass


class SearchError(WizardError):
    pass


class InvalidDownload(WizardError):
    pass


class InvalidFile(WizardError):
    pass


class FoundVaraint(WizardError):
    pass


class APKWizard(object):
    """ The wizard will allow for simplified APK management for the required packages

    Args:
        db_wrapper: Database wrapper
        storage: Abstract storage element for interacting with storage medium

    Attributes:
        _db_wrapper: Database wrapper
        storage: Abstract storage element for interacting with storage medium
    """
    storage: AbstractAPKStorage

    def __init__(self, db_wrapper: DbWrapper, storage: AbstractAPKStorage):
        self.storage: AbstractAPKStorage = storage
        self._db_wrapper: DbWrapper = db_wrapper

    async def apk_all_search(self) -> None:
        """Search for updates for any required package"""
        try:
            await self.find_latest_pogo(APKArch.armeabi_v7a)
            await self.find_latest_pogo(APKArch.arm64_v8a)
        except SearchError:
            # The error has already been logged as a warning
            pass
        await self.find_latest_rgc(APKArch.noarch)
        await self.find_latest_pd(APKArch.noarch)

    async def apk_download(self, package: APKType, architecture: APKArch) -> None:
        """Download a specific package

        Args:
            package (APKType): Package to download
            architecture (APKArch): Architecture of the package to download
        """
        if package == APKType.pogo and await self.find_latest_pogo(architecture):
            await self.download_pogo(architecture)
        elif package == APKType.rgc and await self.find_latest_rgc(architecture):
            await self.download_rgc(architecture)
        elif package == APKType.pd and await self.find_latest_pd(architecture):
            await self.download_pd(architecture)

    async def apk_nonblocking_download(self) -> None:
        """Download all packages"""
        download_tasks: List[Task] = []
        loop = asyncio.get_running_loop()
        for arch in APKArch:
            if arch == APKArch.noarch:
                continue
            try:
                if await self.find_latest_pogo(arch):
                    download_tasks.append(loop.create_task(self.download_pogo(arch)))
            except InvalidDownload:
                pass
        if await self.find_latest_rgc(APKArch.noarch):
            download_tasks.append(loop.create_task(self.download_rgc(APKArch.noarch)))
        if await self.find_latest_pd(APKArch.noarch):
            download_tasks.append(loop.create_task(self.download_pd(APKArch.noarch)))
        for download_task in download_tasks:
            await download_task

    async def apk_search(self, package: APKType, architecture: APKArch) -> None:
        """ Search for a specific package

        Args:
            package (APKType): Package to search
            architecture (APKArch): Architecture of the package to search
        """
        # TODO: Async calls
        if package == APKType.pogo:
            try:
                await self.find_latest_pogo(architecture)
            except SearchError:
                # The error has already been logged as a warning
                pass
        elif package == APKType.rgc:
            await self.find_latest_rgc(architecture)
        elif package == APKType.pd:
            await self.find_latest_pd(architecture)

    async def download_pogo(self, architecture: APKArch) -> None:
        """ Download the package com.nianticlabs.pokemongo

        Determine if theres a newer version of com.nianticlabs.pokemongo.  If a new version exists, validate it is
        supported by MAD.  If the release is supported download and save to the storage interface

        Args:
            architecture (APKArch): Architecture of the package to download
        """
        if not self.storage.token:
            logger.warning(
                "The MADdev API token (maddev_api_token) has not been configured in config.ini."
                " Please configure this to use the wizard for downloading PokemonGo."
            )
            raise NoMaddevApiTokenError()
        try:
            latest_pogo_info, version_str = await self.find_latest_pogo(architecture)
        except SearchError:
            # The error has already been logged as a warning
            return None
        if latest_pogo_info is None:
            logger.warning('Unable to find latest data for PoGo. Try again later')
            return None
        try:
            current_version = await self.storage.get_current_version(APKType.pogo, architecture)
        except FileNotFoundError:
            current_version = None
        if current_version and current_version == version_str:
            logger.info("Latest version of PoGO is already installed")
            return None
        logger.info("Starting download of PoGo [{}] {}", version_str, architecture.name)
        update_data = {
            'download_status': 1
        }

        async with self._db_wrapper as session, session:
            try:
                await MadApkAutosearchHelper.insert_or_update(session, APKType.pogo, architecture, update_data)
                await session.commit()
            except Exception as e:
                logger.warning("Failed insert/update apk in DB: {}", e)

        data: bytearray = bytearray()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2400)) as session:
            async with session.get(latest_pogo_info.download_url, headers=HEADERS, allow_redirects=True) as resp:
                while True:
                    chunk = await resp.content.read(4096)
                    if not chunk:
                        break
                    data += bytearray(chunk)
        if not data:
            logger.warning("Unable to successfully download PoGo")
        try:
            package_importer: PackageImporter = PackageImporter(APKType.pogo, architecture, self.storage,
                                                                io.BytesIO(data),
                                                                'application/vnd.android.package-archive',
                                                                version=version_str)
            await package_importer.import_configured()
        except Exception as err:
            logger.warning("Unable to import the APK. {}", err)
        finally:
            update_data["download_status"] = 0
            async with self._db_wrapper as session, session:
                try:
                    await MadApkAutosearchHelper.insert_or_update(session, APKType.pogo, architecture, update_data)
                    await session.commit()
                except Exception as e:
                    logger.warning("Failed insert/update apk in DB: {}", e)

    async def download_pd(self, architecture: APKArch) -> None:
        """ Download the package com.mad.pogodroid

        Args:
            architecture (APKArch): Architecture of the package to download
        """
        logger.info("Downloading latest PogoDroid")
        await self.__download_simple(APKType.pd, architecture)

    async def __download_simple(self, package: APKType, architecture: APKArch) -> None:
        """ Downloads the package via requests

        Determine if there is a newer version of the package.  If there is a newer version, download and save to the
        storage interface

        Args:
            package (APKType): Package to download
            architecture (APKArch): Architecture of the package to download
        """
        latest_data: Optional[MadApkAutosearch] = await self.get_latest(package, architecture)
        try:
            current_version = await self.storage.get_current_version(package, architecture)
        except FileNotFoundError:
            current_version = None
        if type(current_version) is not str:
            current_version = None
        if not latest_data or latest_data.url is None:
            await self.apk_search(package, architecture)
            latest_data: Optional[MadApkAutosearch] = await self.get_latest(package, architecture)
        if not latest_data:
            logger.warning('Unable to find latest data')
        elif current_version and 'size' in current_version and current_version == latest_data.version:
            logger.info('Latest version already installed')
        else:
            update_data = {
                'download_status': 1
            }
            async with self._db_wrapper as session, session:
                try:
                    await MadApkAutosearchHelper.insert_or_update(session, package, architecture, update_data)
                    await session.commit()
                except Exception as e:
                    logger.warning("Failed insert/update apk in DB: {}", e)

            try:
                retries: int = 0
                successful: bool = False
                while retries < MAX_RETRIES and not successful:
                    response = await RestHelper.send_get(latest_data.url, headers=APK_HEADERS, get_raw_body=True,
                                                         timeout=360)
                    if response.status_code == 200:
                        downloaded_file = io.BytesIO(response.result_body)
                        if downloaded_file and downloaded_file.getbuffer().nbytes > 0:
                            package_importer: PackageImporter = PackageImporter(package, architecture, self.storage,
                                downloaded_file,
                                'application/vnd.android.package-archive'
                            )
                            successful = await package_importer.import_configured()
                    if not successful:
                        logger.info("Issue downloading apk")
                        retries += 1
                        if retries < MAX_RETRIES:
                            logger.warning('Unable to successfully download the APK')
            except Exception as e:  # noqa: E722
                logger.exception(e)
                logger.warning('Unable to download the file @ {}', latest_data.url)
            finally:
                update_data['download_status'] = 0
                async with self._db_wrapper as session, session:
                    try:
                        await MadApkAutosearchHelper.insert_or_update(session, package, architecture, update_data)
                        await session.commit()
                    except Exception as e:
                        logger.warning("Failed insert/update apk in DB: {}", e)

    async def download_rgc(self, architecture: APKArch) -> None:
        """ Download the package de.grennith.rgc.remotegpscontroller

        Args:
            architecture (APKArch): Architecture of the package to download
        """
        logger.info("Downloading latest RGC")
        await self.__download_simple(APKType.rgc, architecture)

    async def __find_latest_head(self, package, architecture, url) -> Optional[bool]:
        """ Determine if there is a newer version by checking the size of the package from the HEAD response

        Args:
            package (APKType): Package to download
            architecture (APKArch): Architecture of the package to download
            url (str): URL to perform the HEAD against
        """
        update_available = False
        try:
            packages = await lookup_package_info(self.storage, package)
        except ValueError:
            packages = None
        curr_info = packages[architecture] if packages else None
        if curr_info:
            installed_size = curr_info.size
            curr_info_logstring = f' (old version {curr_info.version} of size {curr_info.size})'
        else:
            installed_size = None
            curr_info_logstring = ''
        timeout = aiohttp.ClientTimeout(total=10)
        mirror_size = -1
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.head(url, headers=APK_HEADERS, allow_redirects=True) as resp:
                    mirror_size = int(resp.headers.get('Content-Length'))

        except (ClientConnectionError, asyncio.TimeoutError) as e:
            logger.warning("Connecting to {} failed: {}", url, str(e))
        except (ClientError, ValueError) as e:
            logger.warning("Request to {} failed: {}", url, e)

        if not curr_info or not installed_size or (installed_size and installed_size != mirror_size):
            logger.info('Newer version found on the mirror of size {}{}', mirror_size, curr_info_logstring)
            update_available = True
        else:
            logger.info('No newer version found (installed version {} of size {})', curr_info.version, curr_info.size)
        await self.set_last_searched(package, architecture, version=str(mirror_size), url=url)
        return update_available

    async def find_latest_pd(self, architecture: APKArch) -> Optional[bool]:
        """ Determine if the package com.mad.pogodroid has an update

        Args:
            architecture (APKArch): Architecture of the package to check
        """
        logger.info('Searching for a new version of PD [{}]', architecture.name)
        return await self.__find_latest_head(APKType.pd, architecture, global_variables.URL_PD_APK)

    async def find_latest_pogo(self, architecture: APKArch) -> Tuple[PackageVariant, str]:
        """ Determine if the package com.nianticlabs.pokemongo has an update

        Args:
            architecture (APKArch): Architecture of the package to check
        """
        logger.info('Searching for a new version of PoGo [{}]', architecture.name)
        mad_supported_pogo = await get_supported_pogo(architecture, self.storage.token)
        available_apks: Optional[PackageBase] = await get_available_versions(mad_supported_pogo[architecture])
        if not available_apks:
            raise WizardError("Unable to query APKMirror download for Pokemon")
        apk_variant = None
        version_str = None
        # This is large enough to be its own function and UT'd
        try:
            for version in mad_supported_pogo[architecture]:
                if version not in available_apks.versions:
                    logger.info("Unable to find Pogo version {} [{}]", version, architecture)
                    continue
                version_data = available_apks.versions[version]
                arch_name = "armeabi-v7a" if architecture == APKArch.armeabi_v7a else "arm64-v8a"
                if arch_name not in version_data.arch:
                    logger.info("Pogo {} [{}] does not exist", version, arch_name)
                    continue
                for variant in version_data.arch[arch_name]:
                    if variant.apk_type != "APK":
                        logger.info("Skipping {} as its not an APK", variant)
                        continue
                    apk_variant = variant
                    version_str = version
                    logger.info("Using Pogo {} [{}]", version, arch_name)
                    raise FoundVaraint
        except FoundVaraint:
            pass
        if not apk_variant:
            logger.warning("Unable to find a suitable download for Pokemon GO [{}]", architecture)
            raise WizardError("Unable to find an APKMirror download for Pokemon GO [{}]", architecture)
        try:
            current_version_str = await self.storage.get_current_version(APKType.pogo, architecture)
        except FileNotFoundError:
            current_version_str = None
        if current_version_str:
            current_version_code = await self.lookup_version_code(current_version_str, architecture)
            if current_version_code is None:
                current_version_code = 0
            latest_vc = apk_variant.version_code
            if latest_vc > current_version_code:
                logger.info("Newer version found: {}", latest_vc)
                await generate_download_url(apk_variant)
                await self.set_last_searched(
                    APKType.pogo,
                    architecture,
                    version=version_str,
                    url=apk_variant.download_url
                )
            elif current_version_code == latest_vc:
                logger.info("Already have the latest version {}", latest_vc)
            else:
                logger.warning("Unable to find a supported version")
        else:
            await generate_download_url(apk_variant)
            await self.set_last_searched(
                APKType.pogo,
                architecture,
                version=version_str,
                url=apk_variant.download_url
            )
        return apk_variant, version_str

    async def find_latest_rgc(self, architecture: APKArch) -> Optional[bool]:
        """ Determine if the package de.grennith.rgc.remotegpscontroller has an update

        Args:
            architecture (APKArch): Architecture of the package to check
        """
        logger.info('Searching for a new version of RGC [{}]', architecture.name)
        return await self.__find_latest_head(APKType.rgc, architecture, global_variables.URL_RGC_APK)

    async def get_latest(self, package: APKType, architecture: APKArch) -> Optional[MadApkAutosearch]:
        """ Determine the latest found version for a given package / architecture

        Args:
            package (APKType): Package to download
            architecture (APKArch): Architecture of the package to download

        Returns (dict):
            Returns the latest version found for the package
        """
        async with self._db_wrapper as session, session:
            return await MadApkAutosearchHelper.get(session, package, architecture)

    def parse_version_codes(self, supported_codes: Dict[APKArch, Dict[str, int]]):
        versions: Dict[APKArch, Dict] = {
            APKArch.armeabi_v7a: {},
            APKArch.arm64_v8a: {}
        }
        for vstr, vcode in supported_codes.items():
            version, arch = vstr.split('_')
            named_arch = APKArch.armeabi_v7a if arch == '32' else APKArch.arm64_v8a
            if named_arch not in versions:
                versions[named_arch] = {}
            versions[named_arch][version] = vcode
        return versions

    async def lookup_version_code(self, version_code: str, arch: APKArch) -> Optional[int]:
        named_arch = '32' if arch == APKArch.armeabi_v7a else '64'
        latest_version = f"{version_code}_{named_arch}"
        data = await get_version_codes()
        return data.get(latest_version, 0)

    async def set_last_searched(self, package: APKType, architecture: APKArch, version: str = None,
                                url: str = None) -> None:
        """ Updates the last search information for the package / architecture

        Args:
            package (APKType): Package to download
            architecture (APKArch): Architecture of the package to download
            version (str): latest version found
            url (str): URL for the download
        """
        data = {}
        if version:
            data['version'] = version
        if url:
            data['url'] = url
        async with self._db_wrapper as session, session:
            try:
                await MadApkAutosearchHelper.insert_or_update(session, package, architecture, data)
                await session.commit()
            except Exception as e:
                logger.warning("Failed insert/update apk in DB: {}", e)


class PackageImporter(object):
    """ Validates and saves the package to the storage interface

    Args:
        package (APKType): Package to save
        architecture (APKArch): Architecture of the package to save
        storage_obj (AbstractAPKStorage): Storage interface for saving
        downloaded_file (io.BytesIO): binary contents to be saved
        mimetype (str): Mimetype of the package
        version (str): Version of the package
    """

    def __init__(self, package: APKType, architecture: APKArch, storage_obj: AbstractAPKStorage,
                 downloaded_file: io.BytesIO, mimetype: str, version: str = None):
        self.package_version: Optional[str] = None
        self.package_name: Optional[str] = None
        self.package_arch: Optional[APKArch] = None
        self._data: io.BytesIO = downloaded_file
        self._storage_obj = storage_obj
        self._mimetype = mimetype
        self._version = version
        self._package = package
        self._arch = architecture

    async def import_configured(self) -> bool:
        if self._mimetype == 'application/vnd.android.package-archive':
            self.package_version, self.package_name = get_apk_info(self._data)
        else:
            self.normalize_package()
            self._mimetype = 'application/zip'
        if self.package_version:
            if not self._version:
                self._version = self.package_version
            try:
                pkg = APKPackage(self.package_name)
            except ValueError:
                raise InvalidFile('Unknown package %s' % self.package_name)
            if pkg.name != self._package.name:
                raise WizardError('Attempted to upload %s as %s' % (pkg.name, self._package.name))
            if self.package_arch and self.package_arch != self._arch and self._arch != APKArch.noarch:
                msg = 'Attempted to upload {} as an invalid architecture.  Expected {} but received {}'
                raise WizardError(msg.format(pkg.name, self._arch.name, self.package_arch.name))
            await self._storage_obj.save_file(self._package, self._arch, self._version, self._mimetype,
                                              self._data, True)
            log_msg = 'New APK uploaded for {} [{}]'
            logger.info(log_msg, self._package.name, self._version)
            return True
        else:
            logger.warning('Unable to determine apk information')
            return False

    def normalize_package(self) -> NoReturn:
        """ Normalize the package

        Validate that only valid APK files are present within the package and have the correct extension.  Exclude the
        DPI APK as it is not relevant to the installation
        """
        pruned_zip = io.BytesIO()
        zout = zipfile.ZipFile(pruned_zip, 'w')
        with zipfile.ZipFile(self._data) as zip_data:
            for item in zip_data.infolist():
                try:
                    with zip_data.open(item, 'r') as fh:
                        bytes_of_apk = io.BytesIO(fh.read())
                        apk = apkutils.APK().from_io(bytes_of_apk).parse_resource()
                        manifest = apk.get_manifest()
                        try:
                            self.package_version = manifest['@android:versionName']
                            self.package_name = manifest['@package']
                        except KeyError:
                            pass
                    try:
                        filename = manifest['@split']
                        if filename[-3:] == 'dpi':
                            continue
                    except KeyError:
                        filename = item.filename
                    else:
                        try:
                            # The architectures use dash but we are required to use underscores
                            self.package_arch = lookup_arch_enum(filename.rsplit('.', 1)[1].replace('-', '_'))
                        except (IndexError, KeyError, ValueError):
                            pass
                    if filename[-3:] != 'apk':
                        filename += '.apk'
                    zout.writestr(filename, zip_data.read(item.filename))
                except (BadZipFile, LargeZipFile):
                    continue
        zout.close()
        if not self.package_version:
            raise InvalidFile('Unable to extract information from file')
        self._data = pruned_zip


@cached(ttl=10 * 60)
async def get_available_versions(versions: List[str]) -> Optional[PackageBase]:
    """Query apkmirror for the available packages"""
    logger.info("Querying APKMirror for the latest releases")
    try:
        available: PackageBase = await package_search_match(
            "https://www.apkmirror.com/apk/niantic-inc/pokemon-go/",
            versions=versions
        )
    except IndexError:
        logger.warning(
            "Unable to query APKMirror. There is probably a recaptcha that needs to be solved and that "
            "functionality is not currently implemented. Please manually download and upload to the wizard"
        )
        raise SearchError("Unable to query APKMirror")
    except Exception as e:
        logger.warning("Failed querying apkmirror: {}", e)
        return None
    else:
        logger.info("Successfully queried APKMirror to get the latest releases")
        return available
