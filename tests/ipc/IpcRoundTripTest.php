<?php

declare(strict_types=1);

namespace altay\netherlib\tests\ipc;

use altay\netherlib\ipc\InterThreadChannelReader;
use altay\netherlib\ipc\InterThreadChannelWriter;
use altay\netherlib\ipc\UserToNetherLibThreadMessageReceiver;
use altay\netherlib\ipc\UserToNetherLibThreadMessageSender;
use altay\netherlib\ipc\NetherLibToUserThreadMessageReceiver;
use altay\netherlib\ipc\NetherLibToUserThreadMessageSender;
use altay\netherlib\ServerInterface;
use altay\netherlib\ServerListener;
use PHPUnit\Framework\TestCase;

final class IpcRoundTripTest extends TestCase{

	public function testEventsRoundTrip() : void{
		$channel = new class implements InterThreadChannelReader, InterThreadChannelWriter{
			/** @var string[] */
			private array $queue = [];
			public function write(string $str) : void{ $this->queue[] = $str; }
			public function read() : ?string{ return array_shift($this->queue); }
		};

		$sender = new NetherLibToUserThreadMessageSender($channel);
		$id = 0x1122334455667788;

		$sender->onSessionOpen($id, "10.0.0.5", 5000, "PUBKEYDATA");
		$sender->onPacketReceive($id, "hello");
		$sender->onPacketAck($id, 99);
		$sender->onPingUpdate($id, 42);
		$sender->onBandwidthUpdate(1000, 2000);
		$sender->onRawPacketReceive("1.2.3.4", 19132, "raw");
		$sender->onSessionClose($id, "timeout");

		$events = [];
		$handler = new class($events) implements ServerListener{
			/** @param mixed[] $events */
			public function __construct(private array &$events){}
			public function onSessionOpen(int $id, string $a, int $p, ?string $k) : void{ $this->events[] = ["open", $id, $a, $p, $k]; }
			public function onSessionClose(int $id, string $r) : void{ $this->events[] = ["close", $id, $r]; }
			public function onPacketReceive(int $id, string $pl) : void{ $this->events[] = ["packet", $id, $pl]; }
			public function onPacketAck(int $id, int $r) : void{ $this->events[] = ["ack", $id, $r]; }
			public function onPingUpdate(int $id, int $ms) : void{ $this->events[] = ["ping", $id, $ms]; }
			public function onRawPacketReceive(string $a, int $p, string $pl) : void{ $this->events[] = ["raw", $a, $p, $pl]; }
			public function onBandwidthUpdate(int $s, int $r) : void{ $this->events[] = ["bw", $s, $r]; }
		};

		$receiver = new NetherLibToUserThreadMessageReceiver($channel);
		while($receiver->handle($handler));

		$id = 0x1122334455667788;
		self::assertSame([
			["open", $id, "10.0.0.5", 5000, "PUBKEYDATA"],
			["packet", $id, "hello"],
			["ack", $id, 99],
			["ping", $id, 42],
			["bw", 1000, 2000],
			["raw", "1.2.3.4", 19132, "raw"],
			["close", $id, "timeout"],
		], $events);
	}

	public function testCommandsRoundTrip() : void{
		$channel = new class implements InterThreadChannelReader, InterThreadChannelWriter{
			/** @var string[] */
			private array $queue = [];
			public function write(string $str) : void{ $this->queue[] = $str; }
			public function read() : ?string{ return array_shift($this->queue); }
		};

		$sender = new UserToNetherLibThreadMessageSender($channel);
		$id = 0x1122334455667788;
		$sender->sendPacket($id, "payload", true, 55);
		$sender->sendPacket($id, "plain", false, null);
		$sender->closeSession($id);
		$sender->shutdown();

		$server = new class implements ServerInterface{
			/** @var mixed[] */
			public array $sent = [];
			/** @var int[] */
			public array $closed = [];
			public function sendPacket(int $sessionId, string $payload, bool $immediate = false, ?int $receiptId = null) : void{ $this->sent[] = [$sessionId, $payload, $immediate, $receiptId]; }
			public function closeSession(int $sessionId) : void{ $this->closed[] = $sessionId; }
			public function setName(string $name) : void{}
			public function blockAddress(string $address, int $timeout = 300) : void{}
			public function unblockAddress(string $address) : void{}
			public function addRawPacketFilter(string $regex) : void{}
			public function sendRaw(string $address, int $port, string $payload) : void{}
		};

		$receiver = new UserToNetherLibThreadMessageReceiver($channel);
		while($receiver->handle($server));

		self::assertSame([[$id, "payload", true, 55], [$id, "plain", false, null]], $server->sent);
		self::assertSame([$id], $server->closed);
		self::assertTrue($receiver->isShutdownRequested());
	}

}
