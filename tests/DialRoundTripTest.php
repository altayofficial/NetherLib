<?php

declare(strict_types=1);

namespace altay\netherlib\tests;

use altay\netherlib\discovery\DiscoveryAcknowledgementPacket;
use altay\netherlib\discovery\DiscoveryCodec;
use altay\netherlib\discovery\DiscoveryMessagePacket;
use altay\netherlib\discovery\DiscoveryPacket;
use altay\netherlib\discovery\DiscoveryReliablePacket;
use altay\netherlib\NetherNetServer;
use altay\netherlib\ServerData;
use altay\netherlib\NetherNetException;
use altay\netherlib\ServerListener;
use PHPUnit\Framework\TestCase;

 /*
 * @group integration
 */
final class DialRoundTripTest extends TestCase{

	use RequiresPeerConnection;

	private const LISTEN_PORT = 17551;
	private const DIAL_PORT = 17552;
	private const SECOND_DIAL_PORT = 17553;
	private const SHARED_MEDIA_PORT = 17554;
	private const LISTEN_NETWORK = 111;
	private const DIAL_NETWORK = 222;
	private const TIMEOUT = 30;

	private ?NetherNetServer $listening = null;
	private ?NetherNetServer $dialling = null;
	private ?NetherNetServer $secondDialling = null;

	protected function setUp() : void{
		$this->requirePeerConnection();
	}

	protected function tearDown() : void{
		$this->secondDialling?->shutdown();
		$this->dialling?->shutdown();
		$this->listening?->shutdown();
	}

	public function testDialledConnectionCarriesPackets() : void{
		$logger = new DiscardingLogger();

		$listeningEvents = new RecordingListener();
		$this->listening = new NetherNetServer($logger, self::LISTEN_NETWORK, new ServerData("Altay", levelName: "World"), "127.0.0.1", self::LISTEN_PORT);
		$this->listening->start($listeningEvents);

		$diallingEvents = new RecordingListener();
		$this->dialling = new NetherNetServer($logger, self::DIAL_NETWORK, new ServerData("Client", levelName: "-"), "127.0.0.1", self::DIAL_PORT);
		$this->dialling->start($diallingEvents);

		//discovery is not what is under test here, so the address is seeded directly
		$this->dialling->getAddressBook()->remember(self::LISTEN_NETWORK, "127.0.0.1", self::LISTEN_PORT, time());

		$session = $this->dialling->dial(self::LISTEN_NETWORK);

		$small = "hello from the dialler";
		//larger than one segment, so it exercises the split and the reassembly on the other side
		$large = str_repeat("A", 300000);

		$deadline = microtime(true) + self::TIMEOUT;
		$sent = false;
		while(microtime(true) < $deadline){
			$this->listening->tick();
			$this->dialling->tick();

			if(!$sent && $diallingEvents->opened && $listeningEvents->opened){
				$session->sendPacket($small);
				$session->sendPacket($large);
				$sent = true;
			}
			if($sent && count($listeningEvents->packets) >= 2){
				break;
			}
			usleep(20000);
		}

		self::assertTrue($diallingEvents->opened, "the dialling side never reported the session open");
		self::assertTrue($listeningEvents->opened, "the listening side never reported the session open");
		self::assertSame([$small, $large], $listeningEvents->packets);
		self::assertSame($session->getId(), $listeningEvents->sessionId);
	}

	public function testDiallersShareTheListenersMediaPort() : void{
		$logger = new DiscardingLogger();

		$listeningEvents = new RecordingListener();
		$this->listening = new NetherNetServer($logger, self::LISTEN_NETWORK, new ServerData("Altay", levelName: "World"), "127.0.0.1", self::LISTEN_PORT);
		$this->listening->setUdpPort(self::SHARED_MEDIA_PORT);
		$this->listening->start($listeningEvents);

		$sessions = [];
		$events = [];
		foreach([[self::DIAL_NETWORK, self::DIAL_PORT], [self::DIAL_NETWORK + 1, self::SECOND_DIAL_PORT]] as $index => [$networkId, $port]){
			$events[$index] = new RecordingListener();
			$transport = new NetherNetServer($logger, $networkId, new ServerData("Client", levelName: "-"), "127.0.0.1", $port);
			$transport->start($events[$index]);
			$transport->getAddressBook()->remember(self::LISTEN_NETWORK, "127.0.0.1", self::LISTEN_PORT, time());
			$sessions[$index] = $transport->dial(self::LISTEN_NETWORK);
			if($index === 0){
				$this->dialling = $transport;
			}else{
				$this->secondDialling = $transport;
			}
		}

		$deadline = microtime(true) + self::TIMEOUT;
		$sent = false;
		while(microtime(true) < $deadline){
			$this->listening->tick();
			$this->dialling?->tick();
			$this->secondDialling?->tick();

			if(!$sent && $events[0]->opened && $events[1]->opened){
				$sessions[0]->sendPacket("first");
				$sessions[1]->sendPacket("second");
				$sent = true;
			}
			if($sent && count($listeningEvents->packets) >= 2){
				break;
			}
			usleep(20000);
		}

		self::assertTrue($events[0]->opened && $events[1]->opened, "a dialler never reported its session open");
		self::assertEqualsCanonicalizing(["first", "second"], $listeningEvents->packets);
		//both reached the listener on the one port, which is where ICE says their media flows
		self::assertSame(self::SHARED_MEDIA_PORT, $sessions[0]->getPort());
		self::assertSame(self::SHARED_MEDIA_PORT, $sessions[1]->getPort());
	}

	public function testATakenMediaPortIsOnlyFatalWhenItWasRequired() : void{
		$squatter = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
		self::assertNotFalse($squatter);
		self::assertTrue(socket_bind($squatter, "127.0.0.1", self::SHARED_MEDIA_PORT));
		try{
			$this->listening = new NetherNetServer(new DiscardingLogger(), self::LISTEN_NETWORK, new ServerData("Altay", levelName: "World"), "127.0.0.1", self::LISTEN_PORT);
			$this->listening->setUdpPort(self::SHARED_MEDIA_PORT, required: false);
			$this->listening->start(new RecordingListener());
			self::assertTrue($this->listening->isRunning());
			$this->listening->shutdown();

			$this->listening = new NetherNetServer(new DiscardingLogger(), self::LISTEN_NETWORK, new ServerData("Altay", levelName: "World"), "127.0.0.1", self::LISTEN_PORT);
			$this->listening->setUdpPort(self::SHARED_MEDIA_PORT);
			$this->expectException(NetherNetException::class);
			$this->listening->start(new RecordingListener());
		}finally{
			socket_close($squatter);
		}
	}

	public function testReliableMessagesAreAcknowledgedEveryTime() : void{
		$this->listening = new NetherNetServer(new DiscardingLogger(), self::LISTEN_NETWORK, new ServerData("Altay", levelName: "World"), "127.0.0.1", self::LISTEN_PORT);
		$this->listening->start(new RecordingListener());

		$peer = self::rawPeer(self::SECOND_DIAL_PORT);
		try{
			$reliable = new DiscoveryReliablePacket(self::LISTEN_NETWORK, 77, 5, self::DIAL_NETWORK, new DiscoveryMessagePacket(self::LISTEN_NETWORK, "Ping"));
			$datagram = DiscoveryCodec::marshal($reliable, self::DIAL_NETWORK);
			//the second copy is what a peer sends when our acknowledgement got lost
			for($i = 0; $i < 2; $i++){
				socket_sendto($peer, $datagram, strlen($datagram), 0, "127.0.0.1", self::LISTEN_PORT);
				$ack = self::receiveFrom($peer, $this->listening);
				self::assertInstanceOf(DiscoveryAcknowledgementPacket::class, $ack);
				self::assertSame(self::DIAL_NETWORK, $ack->recipientId);
				self::assertSame(77, $ack->nonce);
				self::assertSame(5, $ack->sequence);
			}
		}finally{
			socket_close($peer);
		}
	}

	public function testDialSignalsReliablyOnceThePeerDoes() : void{
		$logger = new DiscardingLogger();

		$listeningEvents = new RecordingListener();
		$this->listening = new NetherNetServer($logger, self::LISTEN_NETWORK, new ServerData("Altay", levelName: "World"), "127.0.0.1", self::LISTEN_PORT);
		$this->listening->start($listeningEvents);

		//nothing we ship starts reliable on its own, retail does. Pretend to be it once so the listener
		//answers the dial reliably, which in turn gets the dialler answering in kind
		$peer = self::rawPeer(self::SECOND_DIAL_PORT);
		try{
			$datagram = DiscoveryCodec::marshal(new DiscoveryReliablePacket(self::LISTEN_NETWORK, 1, 1, self::DIAL_NETWORK, new DiscoveryMessagePacket(self::LISTEN_NETWORK, "Ping")), self::DIAL_NETWORK);
			socket_sendto($peer, $datagram, strlen($datagram), 0, "127.0.0.1", self::LISTEN_PORT);
			self::assertInstanceOf(DiscoveryAcknowledgementPacket::class, self::receiveFrom($peer, $this->listening));
		}finally{
			socket_close($peer);
		}

		$diallingEvents = new RecordingListener();
		$this->dialling = new NetherNetServer($logger, self::DIAL_NETWORK, new ServerData("Client", levelName: "-"), "127.0.0.1", self::DIAL_PORT);
		$this->dialling->start($diallingEvents);
		$this->dialling->getAddressBook()->remember(self::LISTEN_NETWORK, "127.0.0.1", self::LISTEN_PORT, time());
		$session = $this->dialling->dial(self::LISTEN_NETWORK);

		$deadline = microtime(true) + self::TIMEOUT;
		$sent = false;
		while(microtime(true) < $deadline){
			$this->listening->tick();
			$this->dialling->tick();
			if(!$sent && $diallingEvents->opened && $listeningEvents->opened){
				$session->sendPacket("over reliable signalling");
				$sent = true;
			}
			if($sent && count($listeningEvents->packets) >= 1){
				break;
			}
			usleep(20000);
		}

		self::assertTrue($diallingEvents->opened && $listeningEvents->opened, "the session never opened");
		self::assertSame(["over reliable signalling"], $listeningEvents->packets);
	}

	private static function rawPeer(int $port) : \Socket{
		$socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
		self::assertNotFalse($socket);
		self::assertTrue(socket_bind($socket, "127.0.0.1", $port));
		socket_set_nonblock($socket);
		return $socket;
	}

	private static function receiveFrom(\Socket $socket, NetherNetServer $server) : ?DiscoveryPacket{
		$deadline = microtime(true) + 5;
		while(microtime(true) < $deadline){
			$server->tick();
			$buffer = "";
			$address = "";
			$port = 0;
			if(@socket_recvfrom($socket, $buffer, 65535, 0, $address, $port) > 0){
				return DiscoveryCodec::unmarshal($buffer)[0] ?? null;
			}
			usleep(10000);
		}
		return null;
	}

}

final class DiscardingLogger implements \Logger{
	public function emergency($message){}
	public function alert($message){}
	public function critical($message){}
	public function error($message){}
	public function warning($message){}
	public function notice($message){}
	public function info($message){}
	public function debug($message){}
	public function log($level, $message){}
	public function logException(\Throwable $e, $trace = null){}
}

final class RecordingListener implements ServerListener{

	public bool $opened = false;
	public ?int $sessionId = null;
	/** @var string[] */
	public array $packets = [];

	public function onSessionOpen(int $sessionId, string $address, int $port, ?string $authenticatedPublicKey) : void{
		$this->opened = true;
		$this->sessionId = $sessionId;
	}

	public function onSessionClose(int $sessionId, string $reason) : void{}

	public function onPacketReceive(int $sessionId, string $payload) : void{
		$this->packets[] = $payload;
	}

	public function onPacketAck(int $sessionId, int $receiptId) : void{}
	public function onPingUpdate(int $sessionId, int $pingMS) : void{}
	public function onBandwidthUpdate(int $bytesSentDiff, int $bytesReceivedDiff) : void{}
}
