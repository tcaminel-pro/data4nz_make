"""
String related utilities

Copyright (C) 2023 Eviden. All rights reserved
"""

import translitcodec
import codecs


def trim_and_replace_non_ascii(s: str) -> str:
    """
    Replace non-ascii characters by the closest ones.
    For exemple,   'à' '–' 'ż' '☺' '€' '°' £ § "
    will become : "'a' '-' 'z' ':-)' 'EUR' ' ' GBP S "
    """
    return str(codecs.encode(s, "translit/long")).strip()


def shorten(data: str, width: int, placeholder="..") -> str:
    """
    Shorten string 'str' to max 'width' caracters, and end with 'placeholder' strif if shortened
    """
    return (
        (data[: width - len(placeholder)] + placeholder) if len(data) > width else data
    )
