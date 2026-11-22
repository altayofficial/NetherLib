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
 * What a server can be told to do. NetherNetServer is the real one; the IPC receiver only needs this much of it.
 */
interface ServerInterface{

	public function sendPacket(int $sessionId, string $payload, bool $immediate = false, ?int $receiptId = null) : void;

	public function closeSession(int $sessionId) : void;

	public function setName(string $name) : void;

	public function blockAddress(string $address, int $timeout = 300) : void;

	public function unblockAddress(string $address) : void;
}
