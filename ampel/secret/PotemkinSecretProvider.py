#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/secret/PotemkinSecretProvider.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 14.08.2020
# Last Modified Date: 20.06.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Type, get_args, get_origin
from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.abstract.Secret import T
from ampel.secret.NamedSecret import NamedSecret


class PotemkinSecretProvider(SecretProvider):

    def tell(self, alias: str, value_type: Type[T]) -> NamedSecret[T]:
        if get_origin(value_type) is tuple:
            value = tuple(t() for t in get_args(value_type))
        else:
            value = value_type() # type: ignore[assignment]
        return NamedSecret(key=alias, value=value) # type: ignore[arg-type]
