#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/legacy/ContentModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 11.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Dict, Union, List, Any
from ampel.common.AmpelUtils import AmpelUtils
from ampel.common.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.flags.AlDocType import AlDocType


@gendocstring
class ContentModel(AmpelBaseModel):
	""" 
	Example: 

	.. sourcecode:: python\n
		{
			"content": {
				"docs": ["TRANSIENT", "COMPOUND", "PHOTOPOINT", "UPPERLIMIT", "T2RECORD"],
				"t2SubSelection": ["SNCOSMO", "CATALOGMATCH"]
			}
		}
	"""


	docs: List[AlDocType]
	t2SubSelection: List[str] = None


	@validator('docs', whole=True, pre=True, always=True)
	def convert_to_enum(cls, v, values, **kwargs):
		""" """

		if not v:
			raise ValueError(
				"transients->content->docs model error\n" +
				'Parameter "docs" cannot be empty'
			)

		# Due to pydantic bug, validators can be called twice
		if AmpelUtils.check_seq_inner_type(v, (int, AlDocType)):
			return v
		else:
			# For convenience and syntax consistency, we accept dicts
			if isinstance(v, dict):
				return cls.logic_dict_to_list("docs", v)

			if not isinstance(v, str) and not AmpelUtils.check_seq_inner_type(v, str):
				raise ValueError(
					"transients->content->docs model error\n" +
					'List values must be string'
				)

		ret = []

		for el in AmpelUtils.iter(v):

			if type(el) is str:
				try:
					ret.append(AlDocType[el])
				except KeyError:
					raise ValueError(
						"transients->select->docs model error\n" +
						"Unknown flag '%s'.\nPlease check class AlDocType for allowed flags" % el
					)
			else:
				raise ValueError("Unexpected format")

		return ret


	@validator('t2SubSelection', pre=True, whole=True)
	def to_seq(cls, v, values, **kwargs):
		""" """

		if AmpelUtils.is_sequence(v):
		
			# Due to pydantic bug, validators can be called twice
			if AmpelUtils.check_seq_inner_type(v, str):
				return v
			else:
				# For convenience and syntax consistency, we accept dicts
				if isinstance(v, dict):
					return cls.logic_dict_to_list("t2SubSelection", v)

				raise ValueError(
					"transients->content->t2SubSelection model error\n" +
					'List values must be string'
				)

		if type(v) is str:
			return [v]


		raise ValueError(
			"transients->content->t2SubSelection unknown format\n" +
			'Offending value: %s' % v
		)


	@classmethod
	def logic_dict_to_list(cls, field, v):
		"""
		For convenience and syntax consistency, we accept docs format such as: 
		{'anyOf': ['a', 'b]} which we convert as simple list
		"""
		if 'anyOf' not in v or 'allOf' in v or len(v) !=1:
			raise ValueError(
				"transients->content->%s model error" % field,
				'Dict value can only contain the key "anyOf"'
			)

		return v['anyOf']


	@validator('t2SubSelection')
	def validate_subselection(cls, t2SubSelection, values, **kwargs):
		"""
		Check transients->content->t2SubSelection
		"""

		# Docs should never be None (checked by prior validators)
		docs = values.get("docs")

		if AlDocType.T2RECORD not in docs:
			raise ValueError(
				"T3 transients->select->docs model error\n" +
				"T2RECORD must be defined in transients->select->docs\n"+
				"when transients->content->t2SubSelection filtering is requested."
			)

		return t2SubSelection
