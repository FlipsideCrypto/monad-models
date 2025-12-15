{% macro create_udf_pubkey_to_address() %}

{% set sql %}
CREATE OR REPLACE FUNCTION {{ target.database }}.utils.udf_pubkey_to_address(compressed_pubkey STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('pycryptodome')
HANDLER = 'pubkey_to_address'
AS
$$
from Crypto.Hash import keccak

# secp256k1 curve parameters
P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
A = 0
B = 7

def modinv(a, m):
    """Extended Euclidean Algorithm for modular inverse"""
    if a < 0:
        a = a % m
    g, x, _ = extended_gcd(a, m)
    if g != 1:
        return None
    return x % m

def extended_gcd(a, b):
    if a == 0:
        return b, 0, 1
    gcd, x1, y1 = extended_gcd(b % a, a)
    x = y1 - (b // a) * x1
    y = x1
    return gcd, x, y

def modular_sqrt(a, p):
    """Tonelli-Shanks algorithm for modular square root"""
    if a == 0:
        return 0
    if pow(a, (p - 1) // 2, p) != 1:
        return None

    # For secp256k1, p % 4 == 3, so we can use the simple formula
    if p % 4 == 3:
        return pow(a, (p + 1) // 4, p)

    # General Tonelli-Shanks (not needed for secp256k1 but included for completeness)
    q = p - 1
    s = 0
    while q % 2 == 0:
        q //= 2
        s += 1

    z = 2
    while pow(z, (p - 1) // 2, p) != p - 1:
        z += 1

    m = s
    c = pow(z, q, p)
    t = pow(a, q, p)
    r = pow(a, (q + 1) // 2, p)

    while True:
        if t == 1:
            return r
        i = 1
        temp = t
        while temp != 1:
            temp = pow(temp, 2, p)
            i += 1
            if i == m:
                return None
        b = pow(c, 1 << (m - i - 1), p)
        m = i
        c = pow(b, 2, p)
        t = (t * c) % p
        r = (r * b) % p

def decompress_pubkey(compressed_hex):
    """Decompress a secp256k1 compressed public key"""
    # Remove 0x prefix if present
    if compressed_hex.startswith('0x') or compressed_hex.startswith('0X'):
        compressed_hex = compressed_hex[2:]

    if len(compressed_hex) != 66:
        return None

    prefix = int(compressed_hex[:2], 16)
    x = int(compressed_hex[2:], 16)

    if prefix not in (2, 3):
        return None

    # Calculate y^2 = x^3 + 7 (mod p)
    y_squared = (pow(x, 3, P) + B) % P
    y = modular_sqrt(y_squared, P)

    if y is None:
        return None

    # Choose the correct y based on parity
    if (prefix == 2 and y % 2 != 0) or (prefix == 3 and y % 2 == 0):
        y = P - y

    # Return uncompressed public key (64 bytes = X || Y, without 04 prefix)
    x_hex = format(x, '064x')
    y_hex = format(y, '064x')
    return x_hex + y_hex

def pubkey_to_address(compressed_pubkey):
    """Convert compressed secp256k1 public key to Ethereum address"""
    if not compressed_pubkey:
        return None

    try:
        # Decompress the public key
        uncompressed = decompress_pubkey(compressed_pubkey)
        if not uncompressed:
            return None

        # Convert to bytes
        pubkey_bytes = bytes.fromhex(uncompressed)

        # Keccak256 hash
        keccak_hash = keccak.new(digest_bits=256)
        keccak_hash.update(pubkey_bytes)
        hash_hex = keccak_hash.hexdigest()

        # Take last 20 bytes (40 hex chars)
        address = '0x' + hash_hex[-40:]
        return address.lower()
    except Exception as e:
        return None
$$
{% endset %}

{% do run_query(sql) %}
{% do log("UDF udf_pubkey_to_address created successfully", info=True) %}

{% endmacro %}
