<?php

declare(strict_types=1);

namespace altay\netherlib\tests;

use altay\netherlib\Credentials;
use altay\netherlib\IceServer;
use PHPUnit\Framework\TestCase;
use altay\datachannel\Context;

final class CredentialsTest extends TestCase{

	private const PAYLOAD = '{"ExpirationInSeconds":86400,"TurnAuthServers":[' .
		'{"Username":"user","Password":"pass","Urls":["turn:turn.example.com:3478"]},' .
		'{"Username":"","Password":"","Urls":["stun:stun.example.com:3478"]}' .
		']}';

	public function testParsesTheServicePayload() : void{
		$credentials = Credentials::fromJson(self::PAYLOAD, 1000);

		self::assertCount(2, $credentials->iceServers);
		self::assertSame(["turn:turn.example.com:3478"], $credentials->iceServers[0]->urls);
		self::assertSame("user", $credentials->iceServers[0]->username);
		self::assertSame("pass", $credentials->iceServers[0]->password);
		self::assertSame(87400, $credentials->expiresAt);
	}

	public function testAcceptsASingleUrlAsAString() : void{
		$server = IceServer::fromArray(["Urls" => "stun:stun.example.com:3478"]);

		self::assertSame(["stun:stun.example.com:3478"], $server->urls);
	}

	public function testRejectsAServerWithoutUrls() : void{
		$this->expectException(\InvalidArgumentException::class);
		IceServer::fromArray(["Username" => "user"]);
	}

	public function testExpiry() : void{
		$credentials = Credentials::fromJson(self::PAYLOAD, 1000);

		self::assertFalse($credentials->isExpired(87399));
		self::assertTrue($credentials->isExpired(87400));
	}

	public function testCredentialsWithoutLifetimeNeverExpire() : void{
		$credentials = Credentials::fromArray(["TurnAuthServers" => []], 1000);

		self::assertNull($credentials->expiresAt);
		self::assertFalse($credentials->isExpired(PHP_INT_MAX));
	}

	public function testTurnUrlsCarryTheirCredentials() : void{
		$credentials = Credentials::fromJson(self::PAYLOAD, 1000);

		self::assertSame("turn:user:pass@turn.example.com:3478", $credentials->iceServers[0]->toUrl("turn:turn.example.com:3478"));
		self::assertSame("turns:a%3Ab:p%40ss@turn.example.com:5349", (new IceServer([], "a:b", "p@ss"))->toUrl("turns:turn.example.com:5349"));
	}

	public function testStunUrlsAreLeftAlone() : void{
		$server = new IceServer(["stun:stun.example.com:3478"], "", "");

		self::assertSame("stun:stun.example.com:3478", $server->toUrl("stun:stun.example.com:3478"));
		self::assertSame("stun:stun.example.com:3478", (new IceServer([]))->toUrl("stun:stun.example.com:3478"));
	}

	public function testUrlsAreAcceptedByTheLibrary() : void{
		if(!extension_loaded("datachannel")){
			self::markTestSkipped("ext-datachannel is not loaded");
		}
		$credentials = Credentials::fromJson(self::PAYLOAD, 1000);
		$urls = [];
		foreach($credentials->iceServers as $server){
			foreach($server->urls as $url){
				$urls[] = $server->toUrl($url);
			}
		}

		$peer = (new Context())->createPeerConnection(["iceServers" => $urls]);
		$peer->close();
		self::assertCount(2, $urls);
	}
}
