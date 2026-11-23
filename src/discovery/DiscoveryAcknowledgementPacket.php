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

namespace altay\netherlib\discovery;

use altay\netherlib\PacketSerializer;

/**
 * Answers a reliable packet. It echoes the nonce and sequence back so the sender can stop repeating it.
 */
final class DiscoveryAcknowledgementPacket extends DiscoveryPacket{

	public const ID = 0x04;

	public function __construct(
		public int $recipientId = 0,
		public int $nonce = 0,
		public int $sequence = 0
	){}

	public function getId() : int{
		return self::ID;
	}

	public function encodePayload(PacketSerializer $out) : void{
		$out->putLLong($this->recipientId);
		$out->putLLong($this->nonce);
		$out->putLLong($this->sequence);
	}

	public function decodePayload(PacketSerializer $in) : void{
		$this->recipientId = $in->getLLong();
		$this->nonce = $in->getLLong();
		$this->sequence = $in->getLLong();
	}
}
