<?php

declare(strict_types=1);

namespace altay\netherlib\tests\discovery;

use altay\netherlib\discovery\DiscoveryAcknowledgementPacket;
use altay\netherlib\discovery\DiscoveryMessagePacket;
use altay\netherlib\discovery\DiscoveryReliablePacket;
use altay\netherlib\discovery\ReliableDelivery;
use PHPUnit\Framework\TestCase;

final class ReliableDeliveryTest extends TestCase{

	public function testRepeatsAreOnlyHandledOnce() : void{
		$delivery = new ReliableDelivery();
		$packet = new DiscoveryReliablePacket(1, 5, 1, 42, new DiscoveryMessagePacket(1, "x"));
		self::assertTrue($delivery->receive(42, $packet, 100));
		self::assertFalse($delivery->receive(42, $packet, 101));
		//same nonce and sequence from someone else is a different packet
		self::assertTrue($delivery->receive(43, $packet, 101));
	}

	public function testRepeatsAreForgottenAfterAWhile() : void{
		$delivery = new ReliableDelivery();
		$packet = new DiscoveryReliablePacket(1, 5, 1, 42, new DiscoveryMessagePacket(1, "x"));
		self::assertTrue($delivery->receive(42, $packet, 100));
		$delivery->expire(200);
		self::assertTrue($delivery->receive(42, $packet, 200));
	}

	public function testOnlyPeersThatSentReliableArePickedForIt() : void{
		$delivery = new ReliableDelivery();
		self::assertFalse($delivery->speaksReliable(42));
		$delivery->receive(42, new DiscoveryReliablePacket(1, 5, 1, 42, new DiscoveryMessagePacket(1, "x")), 100);
		self::assertTrue($delivery->speaksReliable(42));
	}

	public function testWrapTakesTheNextSequence() : void{
		$delivery = new ReliableDelivery();
		$first = $delivery->wrap(new DiscoveryMessagePacket(42, "a"), 1);
		$second = $delivery->wrap(new DiscoveryMessagePacket(42, "b"), 1);
		self::assertSame($first->nonce, $second->nonce);
		self::assertSame($first->sequence + 1, $second->sequence);
		self::assertSame(42, $first->recipientId);
		self::assertSame(1, $first->senderId);
	}

	public function testRetransmitsUntilAcknowledged() : void{
		$delivery = new ReliableDelivery();
		$packet = $delivery->wrap(new DiscoveryMessagePacket(42, "a"), 1);
		$delivery->track($packet, "datagram", "10.0.0.2", 7551);

		$now = microtime(true);
		self::assertSame([], $delivery->retransmit($now));
		self::assertSame([["datagram", "10.0.0.2", 7551]], $delivery->retransmit($now + 1.5));

		self::assertFalse($delivery->acknowledge(43, new DiscoveryAcknowledgementPacket(1, $packet->nonce, $packet->sequence)));
		self::assertTrue($delivery->acknowledge(42, new DiscoveryAcknowledgementPacket(1, $packet->nonce, $packet->sequence)));
		self::assertSame([], $delivery->retransmit($now + 10));
		self::assertSame(0, $delivery->getOutgoingCount());
	}

	public function testGivesUpAfterSomeAttempts() : void{
		$delivery = new ReliableDelivery();
		$delivery->track($delivery->wrap(new DiscoveryMessagePacket(42, "a"), 1), "datagram", "10.0.0.2", 7551);
		$now = microtime(true);
		$sent = 0;
		for($i = 1; $i <= 10; $i++){
			$sent += count($delivery->retransmit($now + $i * 2));
		}
		self::assertSame(4, $sent);
		self::assertSame(0, $delivery->getOutgoingCount());
	}
}
