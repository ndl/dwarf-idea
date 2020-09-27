Intro
=====

DwarfIdea is a storage format, libraries and tools for fully offline (= without Internet connectivity) network-based location, such as determining your coarse position using the IDs of nearby mobile towers or WiFi access points.

Note that the question of obtaining this data is **out of scope**, it's assumed that the data is already acquired and just needs to be efficiently stored + queried.

Given the large volume of the data involved (on the order of 100M entries for publicly available datasets), all network-based location providers I'm aware of either:
1. Don't keep the necessary data locally at all and perform server requests to do the lookup - and, thus, need Internet access (see https://github.com/microg/IchnaeaNlpBackend).
2. Assume that only the relevant subset of the data is stored locally, for example by acquiring it locally (see https://github.com/n76/wifi_backend) or by preparing in advance and storing locally the relevant subset of the data, for example at the individual countries level (see https://github.com/n76/lacells-creator).

The goal of this project is to develop the storage format that allows keeping the necessary data **for the whole world** on the typical mobile device instead of limiting the data to any particular set of regions or countries.

The project consists of:
* [dwarf-idea-tools](https://github.com/ndl/dwarf-idea-tools): the tooling to convert the location data into DwarfIdea format.
* [dwarf-idea](https://github.com/ndl/dwarf-idea): Java library to lookup the entries in files stored in DwarfIdea format on Android devices.
* Patched [wifi_backend](https://github.com/ndl/wifi_backend) and [Local-GSM-Backend](https://github.com/ndl/Local-GSM-Backend) backends so that they can use the data in DwarfIdea format via the corresponding library.

License
=======

Copyright (C) 2019 - 2020 Alexander Tsvyashchenko <android@endl.ch>

The license is [GPL v3](https://www.gnu.org/licenses/gpl-3.0.html) both for the tooling and the library.

FSE implementation in [dwarf-idea](https://github.com/ndl/dwarf-idea) is based on ZSTD decompressor from https://github.com/airlift/aircompressor and is licensed under Apache 2.0, similarly to the original code. See the individual files in the package for more details. Because aircompressor code uses `sun.misc.Unsafe` interface not available on Android, using the library 'as is' wasn't an option, all access to the data must have been rewritten.

Supported data types and providers
==================================

Currently the following data types and providers are supported:
* Mobile towers (cells IDs): [MLS Cells IDs data](https://location.services.mozilla.com/downloads), [OpenCellID data](https://opencellid.org/downloads.php), [openBmap data (Mylnikov's compilation)](https://www.mylnikov.org/archives/1170).
* WiFi access points (MACs): [OpenWifi.su data (Mylnikov's compilation)](https://www.mylnikov.org/archives/1170).

Adding extra data types and providers should be quite easy.

How big the savings are?
========================

TL;DR: 5x for cell IDs, 2.5x for WiFi MACs.

The combination of three cells IDs datasets listed above, after some filtering and deduplication, results in 63M entries. Each entry has Cell ID key (10 bytes of MCC:MNC:LAC:CELL) + LAT / LON coordinates (2 * 4 bytes if stored as float32) + radius (2 bytes if stored as int16) + number of samples (1 byte if stored as int8) = 21 bytes per entry.

63M * 21 bytes = 1.3 GB, which is not insurmountable given the storage volumes provided by the current mobile devices, but still is high. Naive compression of the data using some standard format cannot be done because we need random access to this data for lookups and the compression factor would be quite limited anyway.

The implemented compression scheme (described below) preserves the ability to do random lookups, yet decreases the data volume to 278 MB, which is 5x compression ratio, or 4.2 bytes per entry. Moreover, the compression ratio increases with more data stored.

The single WiFi MACs dataset listed above, after some filtering and deduplication, results in 20M entries. Each entry has MAC key (6 bytes) + LAT / LON coordinates (2 * 4 bytes if stored as float32) = 14 bytes per entry, or 270 MB total.

DwarfIdea size is 108 MB, which is 2.5 compression ratio, or 5.6 bytes per entry.

Why this name?
==============

`Dwarf` is for the size, as in [Dwarf star](https://en.wikipedia.org/wiki/Dwarf_star), `Idea` is from the old Russian joke based on the word play (I-de-ya, meaning "so, where am I?").

Implementation
==============

The following key assumptions are made:
* It's OK to loose some accuracy in lat / lon positions of the entries. This is justified by the fact that their original positions are often determined using GPS of the phones or other devices nearby, therefore contain both GPS errors + uncertainty due to the devices being only in the viccinity of the relevant point. The max spatial error is one of the algorithms parameters that's specified by the user. For the numbers shown here both max cell ID and WiFi MAC location errors were set to 50m.
* It's OK to loose some accuracy when storing the radius + number of samples. Given these are used only for weighting in the final location calculation, the exact values are not critically important. The current storage scheme packs both into a single byte.
* There's some spatial locality in the data. This is definitely true for cells IDs, where parts of the key are explicitly representing the country / region. While generally this is not true for WiFi MACs, the compression scheme based on this assumption still works OK(ish).

The key ideas of the implementation are:
* Store the data sorted by the key.
* The data is stored in individually compressed blocks, so that to access the data only this particular block needs to be decompressed.
* The index that contains the first key of each block is stored in uncompressed form, so that the binary search to locate the necessary block to decompress can be performed quickly.
* In each block the keys are stored as variable delta-encoded sequences.
* Each block stores the extents of the spatial region it represents.
* The coordinates for each entry are stored w.r.t. the block extents.
* The length of coordinates representation is selected for each block individually but fixed for the whole block to achieve the sufficient accuracy specified by the user.

The alternative, tree-based implementation was also experimented with, but was found to be both more complex and less efficient.

The compression itself uses BWTS + SBRT + ZRLT + FSE algorithms applied separately to each type of data stored (that is, delta-encoded keys, extent-ranged coordinates and "extra data" such as radius + number of samples). FSE statistics for compression are gathered per each particular data type over the whole dataset.

To implement the algorithms mentioned above the [Kanzi](https://github.com/flanglet/kanzi-cpp) and [FSE](https://github.com/Cyan4973/FiniteStateEntropy) libraries are used.

Possible improvements
=====================

* The current parameters of the database builder, while producing reasonable results, might still benefit from further tuning.
* The current spatial error calculation is too conservative, with requested max error 50m the actual max error is 25m and the average error is less than 10m, we could increase the compression ratio by utilizing the allowed error more consistently.
* The integration of DwarfIdea (Java lookup library) into [wifi_backend](https://github.com/ndl/wifi_backend) and [Local-GSM-Backend](https://github.com/ndl/Local-GSM-Backend) should probably be migrated to [DejaVu](https://github.com/n76/DejaVu) unified backend.
* Only DwarfIdea (Java lookup library) is at least somewhat tested (using sort-of-regression test), the rest of the code doesn't have test coverage at all.
* The code could definitely benefit from more comments and documentationB.
* During DwarfIdea construction all data is loaded and stored in memory, which means one needs a lot of RAM. In principle, streaming approaches are also possible, but might complicate the code so it's not clear whether it's currently really worth it. The situation might change if at some point significantly larger public datasets become available.
* WiFi MACs compression ratio is much lower than for cells IDs due to the violation of locality assumption. It might be useful to switch from the per block extents-based coordinates storage, which is unlikely to be very beneficial for spatially non-local data, to modeling the distribution of positions explicitly. That is, given the density of positions is highly non-uniform, it might pay off to model this density. For example, we could compute the global split of the positions into non-equal areas, with smaller areas for higher-density regions, and then specify the area index + residuals inside the coordinate blocks, where the residuals for higher-density areas should be shorter. Alternatively (or in addition to) we could use variable-length area indices to shorten the representation of the most common areas. However, similar to the point above - it likely makes sense to invest the time into this only if much larger-scale public datasets become available, as the storage cost of ~100 MB for currently available data is likely to be already acceptable for most use cases.
