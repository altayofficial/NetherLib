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

namespace altay\netherlib\utils;

/**
 * Several openssl_*() functions load openssl.cnf before doing anything, and the static PHP builds
 * used by most servers point at a path that doesn't exist on the host. That makes CSR creation and
 * key export fail with "BIO routines::no such file" on machines that are otherwise perfectly fine,
 * so we ship a config of our own and hand it to those calls.
 */
final class OpenSslConfig{

	private static ?string $path = null;

	private function __construct(){}

	public static function path() : string{
		return self::$path ??= self::resolvePath();
	}

	/**
	 * @return array<string, string>
	 */
	public static function options() : array{
		$path = self::path();
		return is_file($path) ? ["config" => $path] : [];
	}

	private static function resolvePath() : string{
		$path = __DIR__ . "/../../resources/openssl.cnf";
		if(!str_starts_with($path, "phar://")){
			return $path;
		}

		//OpenSSL opens the file with plain C I/O, so a path inside a phar is as good as missing
		$contents = @file_get_contents($path);
		if($contents === false){
			return $path;
		}
		$extracted = sys_get_temp_dir() . "/netherlib-openssl-" . hash("crc32b", $contents) . ".cnf";
		if(!is_file($extracted) && @file_put_contents($extracted, $contents) === false){
			return $path;
		}
		return $extracted;
	}
}
