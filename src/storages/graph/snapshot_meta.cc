/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/storages/snapshot_meta.h"

#include "neug/storages/module/module_factory.h"

#include <fstream>
#include <string>

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

#include "neug/storages/workspace.h"

namespace neug {

// ---------------------------------------------------------------------------
// Module map access
// ---------------------------------------------------------------------------

std::optional<ModuleDescriptor> SnapshotMeta::module(
    const std::string& key) const {
  auto it = modules_.find(key);
  if (it == modules_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void SnapshotMeta::set_module(const std::string& key, ModuleDescriptor desc) {
  modules_[key] = std::move(desc);
}

void SnapshotMeta::remove_module(const std::string& key) {
  modules_.erase(key);
}

bool SnapshotMeta::has_module(const std::string& key) const {
  return modules_.count(key) > 0;
}

const std::unordered_map<std::string, ModuleDescriptor>& SnapshotMeta::modules()
    const {
  return modules_;
}

// ---------------------------------------------------------------------------
// Module overrides
// ---------------------------------------------------------------------------

void SnapshotMeta::Open(Checkpoint& ckp, const ModuleDescriptor& /*descriptor*/,
                        MemoryLevel /*level*/) {
  // SnapshotMeta lives at the canonical checkpoint meta path.
  std::string meta_path = ckp.meta_path();
  std::ifstream ifs(meta_path);
  if (!ifs.is_open()) {
    LOG(WARNING) << "SnapshotMeta::Open: cannot open " << meta_path;
    return;
  }

  rapidjson::IStreamWrapper isw(ifs);
  rapidjson::Document doc;
  doc.ParseStream(isw);

  if (doc.HasParseError() || !doc.IsObject()) {
    LOG(ERROR) << "SnapshotMeta::Open: invalid JSON in " << meta_path;
    return;
  }

  modules_.clear();
  if (doc.HasMember("modules") && doc["modules"].IsObject()) {
    for (auto& kv : doc["modules"].GetObject()) {
      if (kv.value.IsObject()) {
        modules_[kv.name.GetString()] = ModuleDescriptor::FromJson(kv.value);
      }
    }
  }
}

ModuleDescriptor SnapshotMeta::Dump(Checkpoint& ckp) {
  // Write to the canonical meta path of the checkpoint (not a UUID subdir).
  std::string meta_path = ckp.meta_path();
  std::ofstream ofs(meta_path);
  if (!ofs.is_open()) {
    LOG(ERROR) << "SnapshotMeta::Dump: cannot open " << meta_path;
    return ModuleDescriptor{};
  }

  rapidjson::Document doc;
  doc.SetObject();
  auto& alloc = doc.GetAllocator();

  doc.AddMember("version", rapidjson::Value(1), alloc);

  rapidjson::Value modules_obj(rapidjson::kObjectType);
  for (const auto& [key, desc] : modules_) {
    rapidjson::Value key_val(
        key.c_str(), static_cast<rapidjson::SizeType>(key.size()), alloc);
    modules_obj.AddMember(key_val, desc.ToJson(alloc), alloc);
  }
  doc.AddMember("modules", modules_obj, alloc);

  rapidjson::OStreamWrapper osw(ofs);
  rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
  doc.Accept(writer);

  ModuleDescriptor self_desc;
  self_desc.path = ckp.path();
  self_desc.size = static_cast<uint64_t>(modules_.size());
  self_desc.type = "snapshot_meta";
  self_desc.version = 1;
  return self_desc;
}

void SnapshotMeta::Close() { modules_.clear(); }

std::unique_ptr<Module> SnapshotMeta::Fork(Checkpoint& ckp, MemoryLevel level) {
  auto desc = Dump(ckp);
  auto copy = std::make_unique<SnapshotMeta>();
  copy->Open(ckp, desc, level);
  return copy;
}

NEUG_REGISTER_MODULE("snapshot_meta", SnapshotMeta);

}  // namespace neug
