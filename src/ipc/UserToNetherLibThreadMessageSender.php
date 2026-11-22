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

namespace altay\netherlib\ipc;

use pocketmine\utils\Binary;

final class UserToNetherLibThreadMessageSender{

	private const FLAG_IMMEDIATE = 0x01;
	private const FLAG_HAS_RECEIPT = 0x02;

	public function __construct(
		private InterThreadChannelWriter $channel
	){}

	public function sendPacket(int $sessionId, string $payload, bool $immediate = false, ?int $receiptId = null) : void{
		$flags = ($immediate ? self::FLAG_IMMEDIATE : 0) | ($receiptId !== null ? self::FLAG_HAS_RECEIPT : 0);
		$this->channel->write(
			chr(CommandId::SEND_PACKET) .
			Binary::writeLLong($sessionId) .
			chr($flags) .
			($receiptId !== null ? Binary::writeLLong($receiptId) : "") .
			$payload
		);
	}

	public function closeSession(int $sessionId) : void{
		$this->channel->write(chr(CommandId::CLOSE_SESSION) . Binary::writeLLong($sessionId));
	}

	public function shutdown() : void{
		$this->channel->write(chr(CommandId::SHUTDOWN));
	}

	public function setName(string $name) : void{
		$this->channel->write(chr(CommandId::SET_NAME) . $name);
	}

	public function blockAddress(string $address, int $timeout) : void{
		$this->channel->write(chr(CommandId::BLOCK_ADDRESS) . Binary::writeLInt($timeout) . $address);
	}

	public function unblockAddress(string $address) : void{
		$this->channel->write(chr(CommandId::UNBLOCK_ADDRESS) . $address);
	}

}
