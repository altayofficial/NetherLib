<?php

declare(strict_types=1);

namespace altay\netherlib\tests\discovery;

use altay\netherlib\discovery\DiscoveryAcknowledgementPacket;
use altay\netherlib\discovery\DiscoveryCodec;
use altay\netherlib\discovery\DiscoveryMessagePacket;
use altay\netherlib\discovery\DiscoveryReliablePacket;
use altay\netherlib\discovery\DiscoveryRequestPacket;
use altay\netherlib\discovery\DiscoveryResponsePacket;
use altay\netherlib\DiscoveryCrypto;
use altay\netherlib\PacketSerializer;
use PHPUnit\Framework\TestCase;

final class DiscoveryCodecTest extends TestCase{

	public function testRequestRoundTrip() : void{
		$encoded = DiscoveryCodec::marshal(new DiscoveryRequestPacket(), 12345678901234);
		$result = DiscoveryCodec::unmarshal($encoded);
		self::assertNotNull($result);
		[$packet, $sender] = $result;
		self::assertInstanceOf(DiscoveryRequestPacket::class, $packet);
		self::assertSame(12345678901234, $sender);
	}

	public function testResponseRoundTrip() : void{
		$payload = random_bytes(40);
		$encoded = DiscoveryCodec::marshal(new DiscoveryResponsePacket($payload), 42);
		$result = DiscoveryCodec::unmarshal($encoded);
		self::assertNotNull($result);
		[$packet, $sender] = $result;
		self::assertInstanceOf(DiscoveryResponsePacket::class, $packet);
		self::assertSame($payload, $packet->applicationData);
		self::assertSame(42, $sender);
	}

	public function testMessageRoundTrip() : void{
		$encoded = DiscoveryCodec::marshal(new DiscoveryMessagePacket(777, "CONNECTREQUEST 5 data"), 42);
		$result = DiscoveryCodec::unmarshal($encoded);
		self::assertNotNull($result);
		[$packet, ] = $result;
		self::assertInstanceOf(DiscoveryMessagePacket::class, $packet);
		self::assertSame(777, $packet->recipientId);
		self::assertSame("CONNECTREQUEST 5 data", $packet->data);
	}

	public function testMessageKeepsBytesPastItsDeclaredLength() : void{
		$body = new PacketSerializer();
		$body->putLShort(DiscoveryMessagePacket::ID);
		$body->putLLong(42);
		$body->put(str_repeat("\x00", 8));
		$body->putLLong(777);
		$body->putByteArray("CONNECTREQUEST 5 da");
		$body->put("ta");
		$payload = pack("v", strlen($body->getBuffer()) + 2) . $body->getBuffer();

		$result = DiscoveryCodec::unmarshal(DiscoveryCrypto::checksum($payload) . DiscoveryCrypto::encrypt($payload));
		self::assertNotNull($result);
		[$packet, ] = $result;
		self::assertInstanceOf(DiscoveryMessagePacket::class, $packet);
		self::assertSame("CONNECTREQUEST 5 data", $packet->data);
	}

	public function testLongMessageDeclaresTheCappedLengthsAndStillRoundTrips() : void{
		$signal = "CONNECTREQUEST 5 " . str_repeat("v=0\r\n", 400);
		$encoded = DiscoveryCodec::marshal(new DiscoveryMessagePacket(777, $signal), 42);

		$payload = DiscoveryCrypto::decrypt(substr($encoded, 32));
		self::assertNotNull($payload);
		self::assertSame(1172, unpack("v", $payload)[1]);
		//length prefix, id, sender, padding and recipient come before the signal's own length
		self::assertSame(DiscoveryMessagePacket::MAX_DECLARED_LENGTH, unpack("V", $payload, 2 + 2 + 8 + 8 + 8)[1]);

		$result = DiscoveryCodec::unmarshal($encoded);
		self::assertNotNull($result);
		[$packet, ] = $result;
		self::assertInstanceOf(DiscoveryMessagePacket::class, $packet);
		self::assertSame($signal, $packet->data);
	}

	public function testRejectsGarbage() : void{
		self::assertNull(DiscoveryCodec::unmarshal("garbage"));
		self::assertNull(DiscoveryCodec::unmarshal(str_repeat("\x00", 64)));
	}

	public function testRejectsCiphertextTheCipherCouldNotHaveProduced() : void{
		//no block cipher output is a partial block, so these never reach the cipher or the MAC
		self::assertNull(DiscoveryCodec::unmarshal(str_repeat("\x00", 32)));
		self::assertNull(DiscoveryCodec::unmarshal(str_repeat("\x00", 32 + 17)));
		self::assertNull(DiscoveryCodec::unmarshal(str_repeat("\x00", 32 + 64 * 1024 + 16)));
	}

	public function testRejectsTamperedChecksum() : void{
		$encoded = DiscoveryCodec::marshal(new DiscoveryRequestPacket(), 1);
		$encoded[0] = $encoded[0] === "\x00" ? "\x01" : "\x00";
		self::assertNull(DiscoveryCodec::unmarshal($encoded));
	}

	public function testReliableRoundTrip() : void{
		$message = new DiscoveryMessagePacket(777, "CANDIDATEADD 5 candidate:1 1 udp 1 10.0.0.1 1 typ host");
		$encoded = DiscoveryCodec::marshal(new DiscoveryReliablePacket(777, -5, 3, 42, $message), 42);
		$result = DiscoveryCodec::unmarshal($encoded);
		self::assertNotNull($result);
		[$packet, $sender] = $result;
		self::assertInstanceOf(DiscoveryReliablePacket::class, $packet);
		self::assertSame(42, $sender);
		self::assertSame(777, $packet->recipientId);
		self::assertSame(-5, $packet->nonce);
		self::assertSame(3, $packet->sequence);
		self::assertSame(42, $packet->senderId);
		self::assertSame(777, $packet->message->recipientId);
		self::assertSame($message->data, $packet->message->data);
	}

	public function testAcknowledgementRoundTrip() : void{
		$result = DiscoveryCodec::unmarshal(DiscoveryCodec::marshal(new DiscoveryAcknowledgementPacket(42, 9, 10), 777));
		self::assertNotNull($result);
		[$packet, $sender] = $result;
		self::assertInstanceOf(DiscoveryAcknowledgementPacket::class, $packet);
		self::assertSame(777, $sender);
		self::assertSame(42, $packet->recipientId);
		self::assertSame(9, $packet->nonce);
		self::assertSame(10, $packet->sequence);
	}

	public function testRejectsAcknowledgementWithoutReliableFlag() : void{
		$body = new PacketSerializer();
		$body->putLShort(DiscoveryAcknowledgementPacket::ID);
		$body->putLLong(42);
		$body->put(str_repeat("\x00", 8));
		$body->putLLong(777);
		$body->putLLong(1);
		$body->putLLong(2);
		$payload = pack("v", strlen($body->getBuffer()) + 2) . $body->getBuffer();

		self::assertNull(DiscoveryCodec::unmarshal(DiscoveryCrypto::checksum($payload) . DiscoveryCrypto::encrypt($payload), $reason));
		self::assertNotNull($reason);
	}

	public function testRejectsReliableCarryingSomethingOtherThanAMessage() : void{
		$body = new PacketSerializer();
		$body->putLShort(DiscoveryReliablePacket::ID);
		$body->putLLong(42);
		$body->putByte(1);
		$body->put(str_repeat("\x00", 7));
		$body->putLLong(777);
		$body->putLLong(1);
		$body->putLLong(2);
		DiscoveryCodec::writeFrame($body, new DiscoveryRequestPacket(), 42);
		$payload = pack("v", strlen($body->getBuffer()) + 2) . $body->getBuffer();

		self::assertNull(DiscoveryCodec::unmarshal(DiscoveryCrypto::checksum($payload) . DiscoveryCrypto::encrypt($payload)));
	}
}
