#include "Join.hpp"

#include <vector>
#include <stdexcept>
#include "constants.hpp"

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));
	mem->reset();
	
	uint input_buffer = MEM_SIZE_IN_PAGE - 1;
	
	for (uint disk_page_id = left_rel.first; disk_page_id < left_rel.second; disk_page_id++) {
		mem->loadFromDisk(disk, disk_page_id, input_buffer);
		Page* input_page = mem->mem_page(input_buffer);
		
		for (uint record_id = 0; record_id < input_page->size(); record_id++) {
			Record record = input_page->get_record(record_id);
			uint bucket_idx = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			Page* bucket_page = mem->mem_page(bucket_idx);
			
			if (bucket_page->full()) {
				uint flushed_page_id = mem->flushToDisk(disk, bucket_idx);
				partitions[bucket_idx].add_left_rel_page(flushed_page_id);
			}
			
			bucket_page->loadRecord(record);
		}
	}
	
	for (uint bucket_idx = 0; bucket_idx < MEM_SIZE_IN_PAGE - 1; bucket_idx++) {
		Page* bucket_page = mem->mem_page(bucket_idx);
		if (!bucket_page->empty()) {
			uint flushed_page_id = mem->flushToDisk(disk, bucket_idx);
			partitions[bucket_idx].add_left_rel_page(flushed_page_id);
		}
	}
	
	mem->reset();
	
	for (uint disk_page_id = right_rel.first; disk_page_id < right_rel.second; disk_page_id++) {
		mem->loadFromDisk(disk, disk_page_id, input_buffer);
		Page* input_page = mem->mem_page(input_buffer);
		
		for (uint record_id = 0; record_id < input_page->size(); record_id++) {
			Record record = input_page->get_record(record_id);
			uint bucket_idx = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			Page* bucket_page = mem->mem_page(bucket_idx);
			
			if (bucket_page->full()) {
				uint flushed_page_id = mem->flushToDisk(disk, bucket_idx);
				partitions[bucket_idx].add_right_rel_page(flushed_page_id);
			}
			
			bucket_page->loadRecord(record);
		}
	}
	
	for (uint bucket_idx = 0; bucket_idx < MEM_SIZE_IN_PAGE - 1; bucket_idx++) {
		Page* bucket_page = mem->mem_page(bucket_idx);
		if (!bucket_page->empty()) {
			uint flushed_page_id = mem->flushToDisk(disk, bucket_idx);
			partitions[bucket_idx].add_right_rel_page(flushed_page_id);
		}
	}
	
	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	vector<uint> result_pages;
	
	for (auto& bucket : partitions) {
		mem->reset();
		
		vector<uint> left_pages = bucket.get_left_rel();
		vector<uint> right_pages = bucket.get_right_rel();
		
		if (left_pages.empty() || right_pages.empty()) {
			continue;
		}
	
		bool left_is_smaller = bucket.num_left_rel_record <= bucket.num_right_rel_record;
		vector<uint>& smaller_pages = left_is_smaller ? left_pages : right_pages;
		vector<uint>& larger_pages = left_is_smaller ? right_pages : left_pages;
		
		uint hash_table_size = MEM_SIZE_IN_PAGE - 2;
		uint input_buffer = MEM_SIZE_IN_PAGE - 2;
		uint result_buffer = MEM_SIZE_IN_PAGE - 1;
		
		for (uint disk_page_id : smaller_pages) {
			mem->loadFromDisk(disk, disk_page_id, input_buffer);
			Page* input_page = mem->mem_page(input_buffer);
			
			for (uint record_id = 0; record_id < input_page->size(); record_id++) {
				Record record = input_page->get_record(record_id);
				uint hash_idx = record.probe_hash() % hash_table_size;
				Page* hash_bucket = mem->mem_page(hash_idx);
				
				if (hash_bucket->full()) {
					throw runtime_error("Hash bucket overflow: smaller relation partition does not fit into memory. This violates the stated assumptions.");
				}
				
				hash_bucket->loadRecord(record);
			}
		}
		
		for (uint disk_page_id : larger_pages) {
			mem->loadFromDisk(disk, disk_page_id, input_buffer);
			Page* input_page = mem->mem_page(input_buffer);
			
			for (uint record_id = 0; record_id < input_page->size(); record_id++) {
				Record probe_record = input_page->get_record(record_id);
				uint hash_idx = probe_record.probe_hash() % hash_table_size;
				Page* hash_bucket = mem->mem_page(hash_idx);
				
				for (uint hash_record_id = 0; hash_record_id < hash_bucket->size(); hash_record_id++) {
					Record hash_record = hash_bucket->get_record(hash_record_id);
					
					if (probe_record == hash_record) {
						Page* result_page = mem->mem_page(result_buffer);
						
						if (result_page->size() >= RECORDS_PER_PAGE - 1) {
							uint flushed_page_id = mem->flushToDisk(disk, result_buffer);
							result_pages.push_back(flushed_page_id);
						}
						
						if (left_is_smaller) {
							result_page->loadPair(hash_record, probe_record);
						} else {
							result_page->loadPair(probe_record, hash_record);
						}
					}
				}
			}
		}
		
		Page* result_page = mem->mem_page(result_buffer);
		if (!result_page->empty()) {
			uint flushed_page_id = mem->flushToDisk(disk, result_buffer);
			result_pages.push_back(flushed_page_id);
		}
	}
	
	return result_pages;
}
