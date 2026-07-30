add_neug_test(runtime_wait_test test_runtime_wait.cc)
# Core transaction protocol tests must build and run regardless of service
# configuration (read-view publication protocol, Phase 0).
add_neug_test(
	transaction_protocol_test
	test_transaction_release_order.cc
	test_read_view_publication.cc)

if (BUILD_HTTP_SERVER)
	add_neug_test(
		transaction_test
		test_insert_transaction.cc
		test_update_transaction.cc
		test_compact_transaction.cc
		test_wal_replay.cc
		test_acid.cc)
endif()
