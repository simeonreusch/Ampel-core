<img align="left" src="https://desycloud.desy.de/index.php/s/99Jkcyzn92rRpHF/preview" width="150" height="150"/>
<br>

# AMPEL-core
<br><br>

# Introduction

AMPEL is a _modular_ and _scalable_ platform with explicit _provenance_ tracking, suited for systematically processing large - possibly complex and heterogeneous - datasets in real-time or not. This includes selecting, analyzing, updating, combining, enriching and reacting to data.

The framework requires analysis and reaction logic to be broken down in adequate indepentent units.
AMPEL is general enough to be applicable in various fields,
it was originaly developped to solve challenges in the context of experimental astrophysics.

AMPEL is written in Python 3.8 and its codebase is fully typed.
 

# Architecture

## Tiers
AMPEL is made of four execution layers (tiers) that replace a traditional pipeline architecture.
<img align="left" src="https://desycloud.desy.de/index.php/s/fz2mnsH4MGEKwfD/preview"/>

The tiers are independently scheduled and the information exchange between tiers occurs via a dedicated database.
The execution layer architecture along with the database structure allow for simple parallelization.

## Units
Each tier is modular and executes so-called "units".

<p align="center">
<img src="https://desycloud.desy.de/index.php/s/P76f9qSWJse8oT7/preview" width=50%/>
</p>

Ampel _base units_ have standardized inputs and ouputs, enforced through abstract classes which units inherit.

## Processes

Every change in AMPEL is triggered by a _process_.
A process executes, at a given date and time, a _processor unit_ that itself runs one or multiple _base units_ with specific configurations.
Information about process executions are registred into the database.
The majority of processes are associated with a specific tier but general processes are possible.

A working AMPEL system will spawn multiple processes, posssibly concurently, accross the four AMPEL tiers.
This will result in the ingestion and analysis of data and the triggering of automated reactions when given data states are detected.

## Channels

_Channels_ are convenient for multi-user or multi-prupose AMPEL systems.
They allow to define and enforce access rights and to privatize processes,
meaning that the output generated by the processes will be only accessible to processes
belonging to the same channel.

Internally, _channels_ are just tags in database documents and ampel configuration files.  
From a user perspective, a channel can be seen as a collection of private processes.

<p align="center">
<img src="https://desycloud.desy.de/index.php/s/YMiGJ2zckgEr54n/preview" width=50%/>
<br/>
Processes associated with a given channel
</p>

Note that within AMPEL, different _channels_ requiring the same computation
will not result in the required computation being performed twice.


# Repositories

The AMPEL code is partitioned in different repositories.  
The only mandatory repository in this list is _ampel-interface_

Public abstract class definitions:  
https://github.com/AmpelProject/Ampel-interface

Specialized classes for Tier 0, capable of handling _alerts_:  
https://github.com/AmpelProject/Ampel-alerts

An add-on that introduces two generic classes of datapoints:  
https://github.com/AmpelProject/Ampel-photometry

Example of an instrument specific implementation:  
https://github.com/AmpelProject/Ampel-ztf

Numerous _base units_, the majority being specific to astronomy:  
https://github.com/AmpelProject/Ampel-contrib-HU/


# Database

MongoDB is used to store data.
The collections have been designed and indexed for fast insertion and query.
Users do not interact with the database directly.
Information exchange is instead regulated through (python) abstract base classes from which units are constructed.
A specific set of internal classes handle database input and output.


# Containers

All AMPEL software, can be combined into one container that defines an instance.
These containers can be used both to process real-time data as well as to reprocess archived data.
The containers themselves should be archived as well.

<!--
Astronomers have during the past century continuously refined tools for
analyzing individual astronomical transients. Simultaneously, progress in instrument and CCD
manufacturing as well as new data processing capabilities have led to a new generation of transient
surveys that can repeatedly scan large volumes of the Universe. With thousands of potential candidates
available, scientists are faced with a new kind of questions: Which transient should I focus on?
What were those things that I dit not look at? Can I have them all?

Ampel is a software framework meant to assist in answering such questions.
In short, Ampel assists in the the transition from studies of individual objects
(based on more or less random selection) to systematically selected samples.
Our design goals are to find a system where past experience (i.e. existing algorithms and code) can consistently be applied to large samples, and with built-in tools for controlling sample selection.
-->

# Installing Ampel

See `<https://ampelproject.github.io/Ampel-core/installing.html>`_.