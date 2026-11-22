<?php

/*
 *
 *      _    _ _
 *     / \  | | |_ __ _ _   _
 *    / _ \ | | __/ _` | | | |
 *   / ___ \| | || (_| | |_| |
 *  /_/   \_\_|\__\__,_|\__, |
 *                       |___/
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Original work by the PocketMine Team.
 * https://www.pocketmine.net/
 *
 * @author Altay Team
 * @link https://github.com/altayofficial
 */

declare(strict_types=1);

namespace altay\netherlib;

/**
 * Everything a running server reports. Only plain values cross this, so the same interface works on the server's own
 * thread and on the other end of the IPC channel.
 */
interface ServerListener{

	public function onSessionOpen(int $sessionId, string $address, int $port, ?string $authenticatedPublicKey) : void;

	public function onSessionClose(int $sessionId, string $reason) : void;

	public function onPacketReceive(int $sessionId, string $payload) : void;

	public function onPacketAck(int $sessionId, int $receiptId) : void;

	public function onPingUpdate(int $sessionId, int $pingMS) : void;

	public function onBandwidthUpdate(int $bytesSentDiff, int $bytesReceivedDiff) : void;
}
