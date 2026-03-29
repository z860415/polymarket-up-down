"""
Proxy 帳戶識別工具。

集中處理 signer、proxy wallet 與 funder 的推導與一致性驗證，
避免交易主線與領取主線出現不同步的地址判斷。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from eth_abi.packed import encode_packed
from eth_utils import keccak, to_bytes, to_checksum_address
from py_builder_relayer_client.signer import Signer

POLYGON_CHAIN_ID = 137
DEFAULT_PROXY_FACTORY = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
PROXY_INIT_CODE_HASH = "0xd21df8dc65880a8606f09fe0ce3df9b8869287ab0b058be05aa9e8af6330a00b"


@dataclass(frozen=True)
class ProxyAccountIdentity:
    """Proxy 帳戶身份解析結果。"""

    signer_address: str
    proxy_wallet: str
    funder_address: str
    wallet_address: str


def same_address(left: str, right: str) -> bool:
    """比較兩個地址是否相同。"""
    if not left or not right:
        return False
    return left.strip().lower() == right.strip().lower()


def _create2_address(bytecode_hash: str, from_address: str, salt: bytes) -> str:
    """依 CREATE2 規則推導 proxy wallet 地址。"""
    raw_bytecode_hash = bytecode_hash[2:] if bytecode_hash.startswith("0x") else bytecode_hash
    raw_from_address = from_address[2:] if from_address.startswith("0x") else from_address
    address_hash = keccak(
        b"\xff"
        + to_bytes(hexstr=f"0x{raw_from_address}")
        + salt
        + to_bytes(hexstr=f"0x{raw_bytecode_hash}")
    )
    return to_checksum_address(address_hash[-20:].hex())


def derive_signer_address(private_key: str) -> str:
    """由私鑰推導 signer 地址。"""
    signer = Signer(private_key, POLYGON_CHAIN_ID)
    return to_checksum_address(signer.address())


def derive_proxy_wallet(signer_address: str, proxy_factory: str = DEFAULT_PROXY_FACTORY) -> str:
    """由 signer 地址推導對應的 proxy wallet。"""
    normalized_signer = to_checksum_address(signer_address)
    normalized_factory = to_checksum_address(proxy_factory)
    salt = keccak(encode_packed(["address"], [normalized_signer]))
    return _create2_address(PROXY_INIT_CODE_HASH, normalized_factory, salt)


def resolve_proxy_account_identity(
    private_key: str,
    *,
    funder_address: str,
    wallet_address: Optional[str] = None,
    proxy_factory: str = DEFAULT_PROXY_FACTORY,
) -> ProxyAccountIdentity:
    """解析正式版 proxy 帳戶身份。"""
    signer_address = derive_signer_address(private_key)
    proxy_wallet = derive_proxy_wallet(signer_address, proxy_factory=proxy_factory)
    normalized_funder = to_checksum_address(funder_address)
    normalized_wallet = to_checksum_address(wallet_address) if wallet_address else signer_address
    return ProxyAccountIdentity(
        signer_address=signer_address,
        proxy_wallet=proxy_wallet,
        funder_address=normalized_funder,
        wallet_address=normalized_wallet,
    )
