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

namespace altay\netherlib\auth;

final class SdpFingerprints{

	private function __construct(){

	}

	/**
	 * Builds the canonical JSON payload covering the DTLS fingerprints of an SDP,
	 * matching the format vanilla signs in the identity assertion.
	 *
	 * Returns null if the SDP has no fingerprints, or if any of its fingerprint lines isn't exactly
	 * "<algorithm> <digest>".
	 */
	public static function canonicalPayload(string $sdp) : ?string{
		if(preg_match_all('/^a=fingerprint:(.*)$/m', $sdp, $lines) === 0){
			return null;
		}
		$parts = [];
		foreach($lines[1] as $line){
			//a line skipped here could still be picked up by the sdp parser and trusted by dtls without
			//ever being covered by the signature, so one malformed line rejects the whole thing
			if(preg_match('/^(\S+) (\S+)\r?$/', $line, $match) !== 1){
				return null;
			}
			$parts[] = '{"algorithm":' . json_encode($match[1]) . ',"digest":' . json_encode($match[2]) . '}';
		}
		return '{"fingerprint":[' . implode(",", $parts) . ']}';
	}
}
