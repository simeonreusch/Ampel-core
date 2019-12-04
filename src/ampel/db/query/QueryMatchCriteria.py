#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/query/QueryMatchCriteria.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.03.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections
from ampel.flags.FlagUtils import FlagUtils

class QueryMatchCriteria:
	"""
    DEPRECATED
	"""


	@staticmethod
	def add_from_list(query, field_name, inlist, should_type=str):
		"""
        DEPRECATED
        
		This function translates a list of str/int (or more generaly 'should_type') 
		representing database matching criteria into a dict instance understandable by mongodb. 
		'inlist' can be nested up to one level.

		It is typically used for queries whose selection criteria are loaded directly 
		from the DB config documents. 

		Parameters:
		-----------
		'inlist': must be a 1-dim or 2-dim list of str/int/floats

		'query': dict that will be used as query criteria (can be empty).
		This dict will be updated with the additional search criteria computed by this method. 
		ValueError will be thrown if 'query' already contains:
		  * the value of 'field_name' as key
		  * the key '$or' (only if the list 'inlist' contains other lists)
		
		'query' value examples: 
			* {}  (empty dict)
			* {'tranId': 'ZTFabc'}  (basic transient ID matching criteria)
			* ...

		'field_name': name (string) of the database field containing the flag values

		'should_type': str / int / floats

		Returns:
		--------
		Dict instance referenced by query (which was updated)

		Examples: 
		---------
		The parameter 'inlist' could be:
		-> either an list of strings that are 'OR' connected: ["HU_SN", "LENSED_SNE"]
		-> either an 2d-list of strings that are 'AND' connected: [["HU_SN", "LENSED_SNE"]]
		  Search criteria would require that documents must contain the channels defined in the list
		-> or list of lists whereby the outer lists are connected with the 'OR' logical operator:
		  [["HU_SN", "LENSED_SNE"], "HU_EARLY_SN"] 

		Applied example: 
		Case 1:
		-> channels = ["HU_EARLY_SN"]
		   finds transient associated with the channel HU_EARLY_SN

		Case 1b:
		-> channels = [["HU_EARLY_SN"]]   -> same as case 1
		   finds transient associated with the channel HU_EARLY_SN

		Case 2:
		-> channels = [["HU_EARLY_SN", "HU_RANDOM"]]
		   finds transient that associated with *both* channels HU_EARLY_SN and HU_RANDOM

		Case 3:
		-> channels = ["HU_EARLY_SN", "HU_RANDOM"]
		   finds transient that associated with *either* the channels HU_EARLY_SN or HU_RANDOM

		Case 4:
		-> channels = ["LENSED_SNE", ["HU_EARLY_SN", "HU_RANDOM"]] 
		   finds transient that associated with 
		   * either with the LENSED_SNE channel
		   * or with both the channels HU_EARLY_SN and HU_RANDOM
		"""
		if inlist is None:
			raise ValueError('"inlist" is None')

		if type(inlist) is should_type:
			query[field_name] = inlist
			return

		if not type(inlist) is list:
			if isinstance(inlist, collections.Sequence):
				inlist = list(inlist)
			else:
				raise ValueError('Illegal "inlist" parameter')

		# Case 1 and 3
		if not QueryMatchCriteria.is_nested_list(inlist):

			query[field_name] = (
				inlist[0] if len(inlist) == 1 # case 1
				else {'$in': inlist} # case 2
			)

		else:

			# Case 2 (and un-necessary case 1b)
			if len(inlist) == 1:
				query[field_name] = (
					inlist[0][0] if len(inlist[0]) == 1 # case 1b
					else {'$all': inlist[0]} # Case 2
				)
				return query

			or_list = []

			for el in inlist:

				if type(el) is should_type:
					or_list.append(
						{field_name: el}
					)
				elif type(el) is list:
					or_list.append(
						{field_name: {'$all': el}}
					)
				else:
					raise ValueError('Illegal type for list member contained in parameter "inlist"')

			if len(or_list) == 1:
				query[field_name] = or_list[0][field_name]
			else:
				query['$or'] = or_list

		return query


	@staticmethod
	def add_from_not_list(query, field_name, inlist, should_type=str):
		"""
        DEPRECATED
        
		ATTENTION: call this method *last* if you want to combine 
		the match criteria generated by this method with the one 
		computed in method add_from_list

		Parameters: see docstring of add_from_list
		Returns: dict instance referenced by parameter 'query' (which was updated)
		"""
		if inlist is None:
			raise ValueError('"inlist" is None')

		# Check if field_name was set previously by enum_flags_to_dbquery()
		if field_name in query and type(query[field_name]) is should_type:

			# If a previous scalar flag matching criteria was set
			# then we need rename it since query[field_name] will become a dict 
			query[field_name] = {'$eq': query[field_name]}

		if type(inlist) is should_type:
			query[field_name]['$ne'] = inlist
			return

		if not type(inlist) is list:
			raise ValueError('Illegal "inlist" parameter')

		# Case 1 and 2
		if not QueryMatchCriteria.is_nested_list(inlist):

			# Robustness
			for el in inlist:
				if not type(el) is should_type:
					raise ValueError('Parameter "inlist" contains element with type %s' % type(el))
					
			if field_name not in query:
				query[field_name] = {}
				
			if len(inlist) == 1:
				query[field_name]['$ne'] = inlist[0]
			else:
				query[field_name]['$not'] = {'$all': inlist}

		else:

			and_list = []

			for el in inlist:


				if type(el) is should_type:
					and_list.append(
						{
							field_name: {'$ne': el}
						}
					)

				elif type(el) is list:

					# Robustness
					for ell in inlist:
						if not type(ell) is should_type:
							raise ValueError('Parameter "inlist" contains element with type %s' % type(ell))

					and_list.append(
						{
							field_name: {
								'$not': {'$all': el}
							}
						}
					)
				else:
					raise ValueError('Parameter "inlist" contains element with type %s' % type(el))

			if len(and_list) == 1:
				query[field_name] = and_list[0][field_name]
			else:
				query['$and'] = and_list

		return query


	@staticmethod
	def is_nested_list(inlist):
		""" """
		# list characterisation (nested or not)
		for el in inlist:
			if type(el) in (list, tuple):
				return True

		return False
