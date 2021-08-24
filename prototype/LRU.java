import java.util.*;

// class Node {
//     int key, val;
//     Node prev, next;
//     public Node(int key, int val) {
//         this.key = key;
//         this.val = val;
//     }
// }

// class DoubleList {
//     private Node head, tail;
//     private int size;   //链表元素数

//     // 初始化双链表数据
//     public DoubleList() {
//         head = new Node(0,0);
//         tail = new Node(0,0);
//         this.head.next = tail;
//         this.tail.prev = head;
//         size = 0;
//     }

//     // 在链表尾部添加节点x, O(1)
//     public void addLast(Node v) {
//         v.prev = tail.prev;
//         v.next = tail;
//         tail.prev.next = v;
//         tail.prev = v;
//         size++;
//     }

//     // 删除链表中的v节点（x 一定存在）
//     // 由于双链表给的目标 Node 节点， 时间复杂度 O(1)
//     public void remove(Node x) {
//         x.prev.next = x.next;
//         x.next.prev = x.prev;
//         size--;
//     }

//     // 删除链表中的第一个节点，并返回该节点 时间复杂度O(1)
//     public Node removeFirst() {
//         if (head.next == tail) {
//             return null;
//         }
//         Node first = head.next;
//         remove(first);
//         // size--;
//         return first;
//     }

//     // 返回链表的长度 时间复杂度
//     public int size() {
//         return size;
//     }

// }


// public class LRUCache {

//     // key -> Node(key, val)
//     private HashMap<Integer, Node> map;
//     // Node(k1, v1) <-> Node(k2, v2)...
//     private DoubleList cache;
//     // 最大容量
//     private int cap;

//     public LRUCache(int capacity) {
//         this.cap = capacity;
//         map = new HashMap<>();
//         cache = new DoubleList();
//     }

//     /** 将某个key提升为最近使用的 */
//     private void makeRecently(int key) {
//         Node x= map.get(key);
//         cache.remove(x);    // 先删除这个节点
//         cache.addLast(x);   // 再添加这个节点
//     }

//     /** 添加最近使用的元素 */
//     private void addRecently(int key, int val) {
//         Node node = new Node(key, val);
//         // 链表尾部就是最近使用的元素
//         cache.addLast(node);
//         map.put(key, node);
//     }

//     /** 删除某一个节点 */
//     private void deleteKey(int key) {
//         Node node = map.get(key);
//         cache.remove(node);
//         map.remove(key);
//     }

//     /** 删除最久未使用的元素 */
//     private void removeLeastRecently() {
//        Node node = cache.removeFirst(); // 链表头部的第一个元素就是最久未使用的
//        map.remove(node.key);
//     }


//     public int get(int key) {
//         if (!map.containsKey(key)) {
//             return -1;
//         }
//         makeRecently(key);
//         return map.get(key).val;
//     }

//     public void put(int key, int value) {
//         if(map.containsKey(key)) {
//             deleteKey(key); // 删除旧数据
//             addRecently(key, value);    // 新插入为最近使用的
//             return;
//         }
//         if (cap == cache.size()) {
//             removeLeastRecently();
//         }

//         addRecently(key, value);
//     }

// }


class LRUCache {
    int cap;
    LinkedHashMap<Integer, Integer> linkedHashCache = new LinkedHashMap<>();
    public LRUCache(int capacity) {
        this.cap = capacity;
    }

    public synchronized int get(int key) {
        if (!linkedHashCache.containsKey(key)) {
            return -1;
        }
        // 将 key 变为最近使用
        makeRecently(key);
        return linkedHashCache.get(key);
    }

    public synchronized void put(int key, int val) {
        if (linkedHashCache.containsKey(key)) {
            // 修改 key 的值
            linkedHashCache.put(key, val);
            makeRecently(key);
            return;
        }

        if (linkedHashCache.size() == this.cap) {
            int oldKey = linkedHashCache.keySet().iterator().next();
            linkedHashCache.remove(oldKey);
        }
        // 将新的 key 添加链表尾部
        linkedHashCache.put(key, val);
    }

    public synchronized void makeRecently(int key) {
        int value = linkedHashCache.get(key);
        linkedHashCache.remove(key);
        linkedHashCache.put(key, value);
    }
}


/**
 * Your LRUCache object will be instantiated and called as such:
 * LRUCache obj = new LRUCache(capacity);
 * int param_1 = obj.get(key);
 * obj.put(key,value);
 */