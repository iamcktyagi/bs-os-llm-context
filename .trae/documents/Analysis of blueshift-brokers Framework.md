I have analyzed the `blueshift-core` package to extract the necessary type definitions and enums, ensuring the LLM has a "rich context" regarding data types and imports.

Here is the **Finalized Plan** to create the `blueshift_context/` folder.

## The Context Folder Structure (`blueshift_context/`)
I will generate **6 files** to serve as a comprehensive knowledge base for the LLM.

### 1. `00_Master_Instructions.md`
The "System Prompt" for the LLM. It defines the specific workflow:
*   **Input**: "Implement broker X using this API doc."
*   **Output**: A single valid Python file.
*   **Constraint**: Must use `ConfigRegistry` and `broker_class_factory`.
*   **Reference**: Explicitly tells the LLM to look at `03_Type_Definitions.md` for available imports and `04_Template.py` for structure.

### 2. `01_Architecture_and_Flow.md`
Explains the "Mental Model" of the framework:
*   How the **Configuration Dictionary** maps to the **Broker Lifecycle**.
*   The Data Flow: `RestAPIBroker` -> `BrokerAPIMixin` -> `API Config` -> `Http Request` -> `Response Parser` -> `Blueshift Object`.
*   The role of `registry` in bridging the gap between static JSON config and dynamic Python logic.

### 3. `02_Configuration_Schemas.md`
The **Rules of the Road**. I will document the JSON schemas with detailed comments:
*   `BROKER_SPEC`: Options, Rate Limits.
*   `API_SPEC`: How to define `endpoints` (GET/POST/PUT/DELETE).
*   `STREAMING_SPEC`: WebSocket connection and message routing.
*   **Crucial**: I will explain the "Expression Language" (e.g., `source: "credentials.api_key"`) which is unique to this framework.

### 4. `03_Type_Definitions.md` (The "Rich" Part)
This file addresses your specific concern about imports and data types. I will compile the definitions I extracted from `blueshift-core`:
*   **Enums**: `OrderSide`, `OrderType`, `OrderValidity`, `ProductType`, `OrderStatus`, `InstrumentType`.
*   **Classes**:
    *   `Asset` / `MarketData`: Full list of attributes (`symbol`, `security_id`, `exchange_name`, etc.) available for use in `mapping`.
    *   `Frequency`: Valid strings (`1m`, `1d`, etc.) and how they map.
*   **Imports**: Exact import paths for all of the above.

### 5. `04_Template.py`
A "Fill-in-the-Blanks" code skeleton.
*   Includes all necessary imports from `blueshift.core`.
*   Sets up the `registry`.
*   Provides empty dictionaries for `BROKER_SPEC`, `API_SPEC`, etc.
*   Includes comments guiding where to put Auth logic vs. Parsing logic.

### 6. `05_Reference_Implementation.py`
The `alpaca.py` example, cleaned up to serve as a perfect "Few-Shot" example. It demonstrates:
*   Complex Auth (Custom headers).
*   Pagination handling.
*   WebSocket subscription logic.

---

## Execution
I will now proceed to **create these 6 files** in the `blueshift_context/` directory.

Once created, your "Goto Command" will simply be:
> "Read the `blueshift_context/` folder. Using `00_Master_Instructions.md` as your guide, implement the **[Broker Name]** integration based on this API documentation: **[Link]**."
