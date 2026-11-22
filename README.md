# NetherLib

NetherNet for Minecraft: Bedrock Edition servers written in PHP: discovery on the local network, HTTP signalling for the Servers tab, the identity checks, and sessions over WebRTC data channels.

The WebRTC part (ICE, DTLS, SCTP) runs natively in [ext-datachannel](https://github.com/altayofficial/ext-datachannel), on libdatachannel's own threads. NetherLib does everything around it: signalling, negotiation limits and timeouts, splitting packets into messages and putting them back together.

## Usage

```php
use altay\netherlib\NetherNetServer;
use altay\netherlib\ServerData;

$server = new NetherNetServer($logger, $networkId, new ServerData("My server", levelName: "world"), "0.0.0.0", NetherNetServer::DISCOVERY_PORT, endpointAddress: "0.0.0.0:19132");
$server->setUdpPort(19132);
$server->start($listener); //a ServerListener

while($running){
	$server->tick();
	usleep(5000);
}
$server->shutdown();
```

To run it on a thread of its own, `altay\netherlib\ipc` has the messages to talk to it from another one.

Every player's media shares the one UDP port given to `setUdpPort()`, and libdatachannel owns that socket. Nothing else can listen on that port, so a Query server has to use a different one.

## Requirements

PHP 8.3 or newer with ext-datachannel, ext-gmp, ext-openssl and ext-sockets.
