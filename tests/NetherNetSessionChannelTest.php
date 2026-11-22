<?php

declare(strict_types=1);

namespace altay\netherlib\tests;

use altay\netherlib\NetherNetSession;
use altay\netherlib\rtc\RtcPeer;
use PHPUnit\Framework\TestCase;

final class NetherNetSessionChannelTest extends TestCase{

	private function session() : NetherNetSession{
		return new NetherNetSession($this->createStub(RtcPeer::class), 1, "127.0.0.1", 0, static function() : void{}, static function() : void{}, static function() : void{});
	}

	private static function classify(string $label, bool $ordered = true, ?int $maxRetransmits = null, ?int $maxPacketLifeTime = null, string $protocol = "") : ?string{
		return NetherNetSession::classifyChannel($label, $protocol, $ordered, $maxRetransmits, $maxPacketLifeTime);
	}

	public function testAcceptsChannelsThatDeliverTheWayTheirNamesSay() : void{
		self::assertSame(NetherNetSession::RELIABLE_CHANNEL, self::classify(NetherNetSession::RELIABLE_CHANNEL));
		self::assertSame(NetherNetSession::UNRELIABLE_CHANNEL, self::classify(NetherNetSession::UNRELIABLE_CHANNEL, ordered: false, maxRetransmits: 0));
	}

	public function testRejectsAReliableChannelThatMayLoseOrReorder() : void{
		self::assertNull(self::classify(NetherNetSession::RELIABLE_CHANNEL, ordered: false));
		self::assertNull(self::classify(NetherNetSession::RELIABLE_CHANNEL, maxRetransmits: 3));
		self::assertNull(self::classify(NetherNetSession::RELIABLE_CHANNEL, maxPacketLifeTime: 100));
	}

	public function testRejectsAnUnreliableChannelThatRetransmitsOrKeepsOrder() : void{
		self::assertNull(self::classify(NetherNetSession::UNRELIABLE_CHANNEL, maxRetransmits: 0));
		self::assertNull(self::classify(NetherNetSession::UNRELIABLE_CHANNEL, ordered: false));
	}

	public function testRejectsAChannelWithASubprotocolOrAnUnknownName() : void{
		self::assertNull(self::classify(NetherNetSession::RELIABLE_CHANNEL, protocol: "x"));
		self::assertNull(self::classify("SomethingElse"));
	}

	public function testMediaAddressMovesWithoutTouchingTheSignallingOne() : void{
		$session = $this->session();
		self::assertSame("127.0.0.1", $session->getAddress());

		$session->setRemoteAddress("203.0.113.7", 50000);

		self::assertSame("203.0.113.7", $session->getAddress());
		self::assertSame(50000, $session->getPort());
		self::assertSame("127.0.0.1", $session->getSignallingAddress());
		self::assertSame(0, $session->getSignallingPort());
	}
}
