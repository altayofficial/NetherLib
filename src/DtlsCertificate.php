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

use altay\netherlib\utils\OpenSslConfig;
use function openssl_csr_new;
use function openssl_csr_sign;
use function openssl_pkey_export;
use function openssl_pkey_new;
use function openssl_x509_export;

/**
 * One self-signed certificate every peer connection presents. Otherwise each connection generates a key pair of its
 * own on its way up, which is wasted work on a server that only ever answers.
 */
final class DtlsCertificate{

	private function __construct(
		public readonly string $certificatePem,
		public readonly string $privateKeyPem
	){}

	/**
	 * @throws \RuntimeException
	 */
	public static function generate(int $days = 30) : self{
		//the nested "ec" form is the one that doesn't read openssl.cnf, see OpenSslConfig
		$key = openssl_pkey_new(["ec" => ["curve_name" => "prime256v1"]]);
		if($key === false){
			throw new \RuntimeException("Failed to generate the DTLS key");
		}
		$options = ["digest_alg" => "sha256"] + OpenSslConfig::options();
		$csr = openssl_csr_new(["commonName" => "WebRTC"], $key, $options);
		if($csr === false){
			throw new \RuntimeException("Failed to build the DTLS certificate request");
		}
		$certificate = openssl_csr_sign($csr, null, $key, $days, $options);
		if($certificate === false || !openssl_x509_export($certificate, $certificatePem)){
			throw new \RuntimeException("Failed to sign the DTLS certificate");
		}
		if(!openssl_pkey_export($key, $privateKeyPem, null, OpenSslConfig::options())){
			throw new \RuntimeException("Failed to export the DTLS private key");
		}
		return new self($certificatePem, $privateKeyPem);
	}
}
