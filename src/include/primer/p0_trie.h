//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) : key_char_(key_char) {}

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   * 移动构造函数。
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept
      : key_char_(other_trie_node.key_char_),
        is_end_(other_trie_node.is_end_),
        children_(std::move(other_trie_node.children_)) {}

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return children_.count(key_char) > 0; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   * 判断这个节点是不是一个孩子节点都没有。在"Remove"函数中这个是很有用的。
   * 我的理解是：这个函数和下面的判断是否是end节点不同，他只是在判断是否有孩子，所以我觉得判断哈希是否为空比较合适。
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return !children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * 插入一个节点到childredn_中，给定了key_char和child的unique_ptr。如果给定的key_char以及存在，则返回nullptr。
   * 如果参数child的key_char和参数中的key_char不同，则返回nullptr
   *
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * 注意，参数child是右值，应该使用移动汉书到chilren_
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * 返回值是一个指向unique_ptr的指针，因为这样可以访问到底层数据而不需要让我们拿着unique_ptr。此外，当出现错误的时候我们可以返回nullptr
   * 注意：
   * unique_ptr 独享所有权
   * unique_ptr对象始终是关联的原始指针的唯一所有者。我们无法复制unique_ptr对象，它只能移动。
   * 由于每个unique_ptr对象都是原始指针的唯一所有者，因此在其析构函数中它直接删除关联的指针，不需要任何参考计数。
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    if (child->key_char_ != key_char || children_.count(key_char) > 0) {
      return nullptr;
    }

    // 插入的时候返回一个pair<iterator,bool> 然后 解析iterator得到unique_ptr，再对Unique_ptr取地址即可。
    return &((children_.insert({key_char, std::move(child)})).first->second);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    auto j = children_.find(key_char);
    return j == children_.end() ? nullptr : &(j->second);
  }
  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    auto j = children_.find(key_char);
    if (j == children_.end()) {
      return;
    }
    children_.erase(j);
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 *
 * 记录了最终的值。
 *
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  // 修改了几个函数的参数和返回值，把T修改成了const T&
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   * 当非终端 TrieNode 转换为终端 TrieNodeWithValue 时使用。
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.

   * TrieNode 的children_map 应该移动到新的TrieNodeWithValue 对象。 由于它包含唯一指针，因此第一个参数是右值引用。

   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * 你应该：
   * 1)调用 TrieNode 的移动构造函数将数据从 TrieNode 移动到TrieNodeWithValue。
   * 2)把这个节点的value_成员变量设置成value
   * 3)把is_end_设置成true.
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */

  // 调用父类的移动构造函数 ： 直接 Father(std::move(father))
  TrieNodeWithValue(TrieNode &&trieNode, const T &value) : TrieNode(std::move(trieNode)), value_(value) {
    TrieNode::is_end_ = true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, const T &value) : TrieNode(key_char), value_(value) { TrieNode::is_end_ = true; }

  /**
   * @brief Destroy the Trie Node With Value object
   */

  // 描述：override保留字表示当前函数重写了基类的虚函数。
  // 目的：
  // 1.在函数比较多的情况下可以提示读者某个函数重写了基类虚函数（表示这个虚函数是从基类继承，不是派生类自己定义的）；
  // 2.强制编译器检查某个函数是否重写基类虚函数，如果没有则报错。

  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  const T &GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 *
 * Trie 是一个并发键值存储。 每个键都是一个字符串及其对应的值，值可以是任意类型。
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  // ReaderWriterLatch latch_;
  std::shared_mutex latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() : root_(new TrieNode('\0')) {}

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   * 如果key为空字符串，则返回false。
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   * 如果key已经存在，返回false。重复的key是不被允许的并且你不应该重写一个以及存在的key。
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * 当你到达了一个key的尾端。
   * 1.如果这个尾端字符不存在，创建一个新的，并加入到父亲的children中
   * 2.如果终端节点是一个TrieNode，则把他转化成一个TrieNodeWithValue通过适当的调用构造函数
   * 3.如果以及存在一个TrieNodeWithValue，则插入失败，并返回false。不要重写。
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * 你可以快速的检查一个节点指针是否是TrieNode还是TrieNodeWithValue通过is_end_
   *
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, const T &value) {
    latch_.lock();

    if (key.empty()) {
      latch_.unlock();
      return false;
    }

    std::unique_ptr<TrieNode> *cur = &root_;
    std::unique_ptr<TrieNode> *j = nullptr;

    for (const char it : key) {
      j = (*cur)->GetChildNode(it);

      if (j == nullptr) {
        (*cur)->InsertChildNode(it, std::make_unique<TrieNode>(it));
        cur = (*cur)->GetChildNode(it);
      } else {
        cur = j;
      }
    }

    if ((*cur)->IsEndNode()) {
      latch_.unlock();
      return false;
    }

    // 把父亲的unique_ptr<TrieNode>*转向，转向一个TrieNodeWithValue,同时释放
    (*cur).reset(static_cast<TrieNode *>(new TrieNodeWithValue(std::move(**cur), value)));
    latch_.unlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * 从Trie中删除键值对，这个函数也会删除那些不再游泳的节点。如果key为空或者没有找到，返回false。
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * 你应该：
   * 1) 找到终端节点通过给定值
   * 2) 如果终端节电没有孩子，把他删除。
   * 3) 递归删除那些没有孩子并且不是终端节点的节点。
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    latch_.lock();

    if (key.empty()) {
      latch_.unlock();
      return false;
    }

    std::unique_ptr<TrieNode> *cur = &root_;
    std::unique_ptr<TrieNode> *j;
    std::vector<std::unique_ptr<TrieNode> *> st;

    for (const char it : key) {
      j = (*cur)->GetChildNode(it);

      if (j == nullptr) {
        latch_.unlock();
        return false;
      }

      st.push_back(cur);
      cur = j;
    }

    // 不是终端节点。
    if (!(*cur)->IsEndNode()) {
      latch_.unlock();
      return false;
    }

    // *cur是终端节点，说明他是TireNodeWithValue需要进行类型转化，
    // 如果有孩子，进行类型转化，如果没孩子，直接删除该节点。
    if ((*cur)->HasChildren()) {
      *cur = std::make_unique<TrieNode>(std::move(**cur));
      latch_.unlock();
      return true;
    }

    (*cur)->SetEndNode(false);

    for (int64_t j = st.size() - 1; j != -1; --j) {
      if ((*cur)->IsEndNode() || (*cur)->HasChildren()) {
        break;
      }

      // 既不是终端节点，也没有任何孩子，则从父亲那里删除。
      (*st[j])->RemoveChildNode((*cur)->GetKeyChar());
      cur = st[j];
    }

    latch_.unlock();
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * 如果key为空，false；如果key不在，false；
   * 如果给定类型 T 与 TrieNode WithValue 中存储的值类型不同
   * 即调用 GetValue<int> 但终端节点保存 std::string），则将 success 设置为 false。
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * 要检查这两个类型是否相同，请将终端 TrieNode 动态转换为 TrieNodeWithValue<T>。
   * 如果转换结果不是 nullptr，则类型 T 是正确的类型。
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    latch_.lock_shared();
    *success = false;

    if (key.empty()) {
      latch_.unlock_shared();
      return {};
    }

    std::unique_ptr<TrieNode> *cur = &root_;
    std::unique_ptr<TrieNode> *j;

    for (const char it : key) {
      j = (*cur)->GetChildNode(it);

      if (j == nullptr) {
        latch_.unlock_shared();
        return {};
      }

      cur = j;
    }

    // 从unique_ptr中拿出原始指针，然后转化成派生类之后调用GetValue,这里的if等价于在判断 TrieNodeWithValue<T> 可由
    // *cur->get() 类型派生出来的。
    if ((*cur)->IsEndNode() && dynamic_cast<TrieNodeWithValue<T> *>(cur->get()) != nullptr) {
      *success = true;
      latch_.unlock_shared();
      return (static_cast<TrieNodeWithValue<T> *>(cur->get()))->GetValue();
    }

    latch_.unlock_shared();
    return {};
  }
};
}  // namespace bustub
