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

use altay\netherlib\ServerInterface;
use pocketmine\utils\Binary;

final class UserToNetherLibThreadMessageReceiver{

	private const FLAG_IMMEDIATE = 0x01;
	private const FLAG_HAS_RECEIPT = 0x02;

	private bool $shutdownRequested = false;

	public function __construct(
		private InterThreadChannelReader $channel
	){}

	public function isShutdownRequested() : bool{
		return $this->shutdownRequested;
	}

	public function handle(ServerInterface $server) : bool{
		$message = $this->channel->read();
		if($message === null){
			return false;
		}

		$id = ord($message[0]);
		$offset = 1;
		switch($id){
			case CommandId::SEND_PACKET:
				$sessionId = Binary::readLLong(substr($message, $offset, 8));
				$offset += 8;
				$flags = ord($message[$offset++]);
				$receiptId = null;
				if(($flags & self::FLAG_HAS_RECEIPT) !== 0){
					$receiptId = Binary::readLLong(substr($message, $offset, 8));
					$offset += 8;
				}
				$server->sendPacket($sessionId, substr($message, $offset), ($flags & self::FLAG_IMMEDIATE) !== 0, $receiptId);
				break;
			case CommandId::CLOSE_SESSION:
				$server->closeSession(Binary::readLLong(substr($message, $offset, 8)));
				break;
			case CommandId::SHUTDOWN:
				$this->shutdownRequested = true;
				break;
			case CommandId::SET_NAME:
				$server->setName(substr($message, $offset));
				break;
			case CommandId::BLOCK_ADDRESS:
				$server->blockAddress(substr($message, $offset + 4), Binary::readLInt(substr($message, $offset, 4)));
				break;
			case CommandId::UNBLOCK_ADDRESS:
				$server->unblockAddress(substr($message, $offset));
				break;
			case CommandId::ADD_RAW_PACKET_FILTER:
				$server->addRawPacketFilter(substr($message, $offset));
				break;
			case CommandId::SEND_RAW:
				$addressLength = ord($message[$offset++]);
				$address = substr($message, $offset, $addressLength);
				$offset += $addressLength;
				$port = Binary::readLShort(substr($message, $offset, 2));
				$server->sendRaw($address, $port, substr($message, $offset + 2));
				break;
		}
		return true;
	}
}
