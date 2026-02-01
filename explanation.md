# Project Explanation: Distributed Key-Value Store with OCC

## 1. Project Overview
**Title:** Distributed Key-Value Store with Optimistic Concurrency Control (OCC)

**Objective:**
The goal of this project is to implement and demonstrate **Optimistic Concurrency Control (OCC)** in a distributed system and compare it against the traditional **Two-Phase Locking (2PL)** approach.

**Key Concept:**
Most databases use "Pessimistic" locking (2PL) — assuming conflicts *will* happen, so they lock data before reading/writing. This project implements "Optimistic" control — assuming conflicts are *rare*, so we let transactions run freely and only check for conflicts at the very end.

---

## 2. System Architecture
The system consists of three main components:

1.  **Coordinator (`coordinator.py`)**:
    *   The "Brain" of the OCC system.
    *   **Responsibilities**: usage of global timestamps, performing validation logic, and determining if a transaction commits or aborts.
    *   *Note*: It does not store the actual data, only metadata about recent transactions.

2.  **Data Nodes (`node.py`)**:
    *   The "Storage" units.
    *   **Responsibilities**: storing key-value pairs (`store = {}`) and handling low-level `GET`/`PUT` requests.
    *   They are **sharded**: Keys are distributed across nodes using a hash function (`hash(key) % num_nodes`).

3.  **Client Library (`client.py`)**:
    *   The interface used by applications.
    *   Handles the logic for buffering writes (during the Read Phase) and communicating with the Coordinator (during the Validation Phase).

---

## 3. How OCC Works (The Core Logic)
This is the most important part to explain. The life of a transaction in my system follows these 3 phases:

### Phase 1: Read Phase (Local Execution)
*   **Action**: The client reads data from nodes.
*   **Key Detail**: All *writes* are **buffered locally** in the client (`self.write_set`). No data is written to the actual nodes yet. This ensures no locks are held on the server, maximizing concurrency.
*   **Tracking**: The client keeps track of every key it reads (`ReadSet`) and every key it wants to write (`WriteSet`).

### Phase 2: Validation Phase (The "Check")
*   **Action**: When the user calls `commit()`, the client sends its `ReadSet` and `WriteSet` to the Coordinator.
*   **Mechanism**: The Coordinator performs **Backward Validation**.
    *   It looks at all transactions that committed *after* our transaction started.
    *   **The Conflict Rule**: If any recently committed transaction wrote to a key that I read, my data is stale. **ABORT**.
    *   *Code Reference*: `coordinator.py`, lines 84-94.

### Phase 3: Write Phase (Apply Changes)
*   **Action**: If validation passes:
    1.  Coordinator assigns a new `Commit Timestamp`.
    2.  Coordinator applies the buffered writes to the actual Data Nodes.
    3.  Transaction is recorded in history for future validations.

---

## 4. Comparison: OCC vs. 2PL
The project also implements 2PL for comparison.

| Feature | Optimistic (OCC) | Pessimistic (2PL) |
| :--- | :--- | :--- |
| **Philosophy** | "Apologize later" (Check at end) | "Ask permission first" (Lock upfront) |
| **Concurrency** | **High**. No locks held during execution. | **Lower**. Readers/Writers block each other. |
| **Best For** | **Read-heavy workloads** (Low conflict). | **Write-heavy workloads** (High conflict). |
| **Failure Mode** | High **Abort Rate** under contention. | **Deadlocks** or High **Wait Times**. |

## 5. Demonstration
I use `benchmark.py` to simulate:
1.  **Low Conflict**: Random keys. OCC wins because it avoids locking overhead.
2.  **High Conflict**: Hot keys. OCC struggles (many aborts), verifying the theory.
