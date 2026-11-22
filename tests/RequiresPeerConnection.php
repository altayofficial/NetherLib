<?php

declare(strict_types=1);

namespace altay\netherlib\tests;

trait RequiresPeerConnection{

	protected function requirePeerConnection() : void{
		if(!extension_loaded("datachannel")){
			self::markTestSkipped("ext-datachannel is not loaded");
		}
	}
}
