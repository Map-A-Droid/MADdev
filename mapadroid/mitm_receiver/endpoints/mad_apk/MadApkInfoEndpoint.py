from typing import Optional, Union

from aiohttp import web

from mapadroid.mad_apk.utils import lookup_package_info, supported_pogo_version
from mapadroid.mitm_receiver.endpoints.AbstractMitmReceiverRootEndpoint import \
    AbstractMitmReceiverRootEndpoint
from mapadroid.utils.apk_enums import APKType
from mapadroid.utils.custom_types import MADPackage, MADPackages
from mapadroid.utils.madGlobals import MadGlobals


class MadApkInfoEndpoint(AbstractMitmReceiverRootEndpoint):
    """
    "/mad_apk/<string:apk_type>"
    "/mad_apk/<string:apk_type>/<string:apk_arch>"
    """
    # Increase timeout to 5minutes to send APK info...
    timeout = 300

    # TODO: Auth/preprocessing for autoconfig?
    async def get(self):
        logger.info("Device {} checking package {} (arch: {}) version", self.request.headers["origin"],
                       self.request.match_info.get('apk_type'), self.request.match_info.get('apk_arch'))
        parsed = self._parse_frontend()
        if type(parsed) == web.Response:
            return parsed
        apk_type, apk_arch = parsed

        try:
            package_info: Optional[Union[MADPackage, MADPackages]] = await lookup_package_info(self._get_storage_obj(),
                                                                                               apk_type, apk_arch)
            if package_info:
                if apk_type == APKType.pogo and not await supported_pogo_version(apk_arch, package_info.version,
                                                                                 MadGlobals.application_args.maddev_api_token):
                    return web.Response(status=406, text='Supported version not installed')
                else:
                    return web.Response(status=200, text=package_info.version)
            else:
                return web.Response(text="", status=404)
        except ValueError:
            return web.Response(text="", status=404)
