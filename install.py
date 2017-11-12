# $Id: install.py 1701 2017-08-15 12:43:07Z mwall $
# installer for MQTT
# Copyright 2014 Matthew Wall

from setup import ExtensionInstaller

def loader():
    return MQTTInstaller()

class MQTTInstaller(ExtensionInstaller):
    def __init__(self):
        super(MQTTInstaller, self).__init__(
            version="0.17-json-fix",
            name='mqtt',
            description='Upload weather data to MQTT server.',
            author="Matthew Wall",
            author_email="mwall@users.sourceforge.net",
            restful_services='user.mqtt.MQTT',
            config={
                'StdRESTful': {
                    'MQTT': {
                        'server_url': 'INSERT_SERVER_URL_HERE'}}},
            files=[('bin/user', ['bin/user/mqtt.py'])]
            )
