# $Id: install.py 1483 2016-04-25 06:53:19Z mwall $
# installer for MQTT
# Copyright 2014 Matthew Wall

from setup import ExtensionInstaller

def loader():
    return MQTTInstaller()

class MQTTInstaller(ExtensionInstaller):
    def __init__(self):
        super(MQTTInstaller, self).__init__(
            version="0.15",
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
