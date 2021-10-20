(ns adp.adp.core
  (use
   [clojure.tools.logging :only (debug info warn error fatal)])
  (require
   [clojure-csv.core :as csv]
   [clojure.tools.cli :refer [parse-opts]]
   [clojure.set :as set]
   [clojure.walk :as walk]
   [me.raynes.fs :as rfs]
   [taoensso.nippy :as nippy]
  )
  (import
   [org.apache.commons.io IOUtils])
  (:gen-class)
)

; Constants

(def valid-modes ["initial" "incremental" "revertpredictions" "revertsensitivity"])

(def support-file-password "¦éÉA-øý¥µ®¬Ó@ÎÕèÛ¥Sõw9ÈáuÀ;Íp")

(def bias-const 1.0)









; Helper functions.

(defmacro ?
  "A useful debugging tool when you can't figure out what's going on: wrap a form with ?, and the form will be printed alongside its result. The result will still be passed along."
  [val]
  `(let [x# ~val]
     (debug '~val '~'= x#)
     x#))

(defmacro info-wrap "Wraps a function call in log messages, both of which are optional."
  [m1 func m2]

  `(do
    (info ~m1)
    (let [result# ~func]
      (do
        (info ~m2)
        result#))))

(defn fix-dble-quotes "Removes the double quotes on both the start and end of the string, if quotes exist in both places."
  [q-string]

  ; If the start and end of the string are both double quotes.
  (if (and
         (= (subs q-string 0 1) "\"")
         (= (subs q-string
                  (- (count q-string) 1)
                  (count q-string))
            "\""))

      ; Return a substring without the double quotes on each end.
      (do
        (debug "Fixed double quotes on" q-string)
        (subs q-string 1 (- (count q-string) 1)))

      ; Return the string unchanged.
      (do
        (debug "Did not find quotes to fix in" q-string)
        q-string)))

(defn right "Returns a substring, starting from the end of a string."
  [source-string char-qty]

  (subs source-string (- (count source-string) char-qty)))

(defn left "Returns a substring, starting from the beginning of a string."
  [source-string char-qty]

  (subs source-string 0 char-qty))

(defn parse-double "Converts data to double more reliably than Clojure's double, which cannot handle strings."
  [item]
  (Double/parseDouble item))

(defn vec-has-val "Boolean function telling whether a vector contains a value."
  [vec val]
  (= (some #{val} vec) val))

(defn parse-cli-path "Parses command-line file path and folder path options."
  [file-path]
  (rfs/normalized-path (fix-dble-quotes file-path)))

(defn parse-cli-field-list "Parses command-line list of field names. Returns a sorted vector of strings."
  [field-list]
  (vec (sort (set (first (csv/parse-csv (fix-dble-quotes (str field-list))))))))

(defn if-nil "An if statement that uses the second argument if the first argument is nil."
  [maybe-nil alternative]
  (if (nil? maybe-nil)
    alternative
    maybe-nil))

(defn str-col "Prints a collection of strings."
  [col]

  (clojure.string/join "\n" (map #(str "\"" % "\"") col)))









; Metadata functions.

(defn stringify-table "Converts all elements in vector of vector to strings."
  [table]

  (mapv
   (fn [row]
     (mapv
      (fn [element]
        (str element))
      row))
   table))

(defn construct-attr-val-table
  [[attributes & values]]
  (vec
   (cons ["field" "distinct-value"]
     (mapcat
      #(for [attribute-index (range (count attributes))
             value (nth % attribute-index)]

         [(nth attributes attribute-index) value])
       values))))

(defn sort-by-field-and-field-data
  [[fields data]]
    (let [pairs (map list fields data)]
        (vector
            (mapv first (sort-by first pairs))
            (mapv (comp sort second) (sort-by first pairs)))))

(defn define-dataset-name "Defines the dataset name by using the supplied name. If a name was not supplied, tries to infer the dataset name from the metadata folder."
  [& {:keys [cli-dataset-name metadata-folder-path]}]

  (do
     (info "Defining dataset name...")
     (let [dataset-name (if (not (nil? cli-dataset-name))
                          cli-dataset-name
                          (let [glob-pattern (clojure.java.io/file metadata-folder-path "*_processing_parameters.nef")
                                file-count (count (rfs/glob glob-pattern))]
                            (? glob-pattern)
                            (? file-count)
                            (cond
                             (= file-count 0)
                             (do (fatal (str "No files matching \"" glob-pattern "\" found in metadata folder."))
                               (throw (Exception. (str "No files ending in \"" glob-pattern "\" found in metadata folder. Please choose a correct metadata folder."))))
                             (= file-count 1)
                             (do
                               (info "Assuming dataset name from found processing parameters file.")

                               ; Use regular expression to retreive dataset name from processing parameters file name.
                               (clojure.string/replace
                                (str
                                 (rfs/base-name
                                  (first (rfs/glob glob-pattern))))
                                "_processing_parameters.nef"
                                ""))
                             (>= file-count 2)
                             (do (fatal (str "Multiple files matching in \"" glob-pattern "\" found in metadata folder."))
                                 (throw (Exception. (str "Multiple files ending in \"" glob-pattern "\" found in metadata folder. Please specify dataset name.")))))))]
       (do
          (info "Finished defining dataset name.")
          dataset-name))))

(defn get-processed-output-field-names "Gets processed field names for all processed ouput fields."
  [processed-field-names]

  ; Get the position of the bias.
  (let [bias-pos (.indexOf processed-field-names "bias")]

    ; Return a subvector starting after the bias.
    (subvec processed-field-names (+ 1 bias-pos))))

(defn get-processed-input-field-names "Gets processed field names for all processed input fields, including bias."
  [processed-field-names]

  ; Get the position of the bias.
  (let [bias-pos (.indexOf processed-field-names "bias")]

    ; Return a subvector that goes to the bias.
    (subvec processed-field-names 0 (+ 1 bias-pos))))










; Processing functions.

(defn abs "Returns the absolute value of the number. Argument is assumed to be a double."
  [num]
    (if (neg? num) (* -1.0 num) num))

(defn max-abs-vec "Finds the largest absolute value in a vector."
  [nums]

  ; Find the largest absolute value.
  (reduce max

   ; Map all numbers to absolute values.
   (map abs nums)))

(defn range-vec "Finds the ranges of values in a vector."
  [nums]

  ; Subtract the minimum from the maximum.
  (-

   ; Find the maximum value.
   (reduce max nums)

   ; Find the minimum value.
   (reduce min nums)))

(defn binary-digit-count "Determines the quantity of binary digits needed to represent a number."
  [number & prev-binary]

  (let [binary (if-nil prev-binary 1)]
    ; If prev-binary is greater than or equal to the number...
    (if (>= binary number)

      ; Return the logarithm, base 2, of binary.
      (/ (Math/log10 binary) (Math/log10 2))

      ; Double binary and check again.
      (recur number (* binary 2)))))


(defn pad-str "Adds character onto a string to reach the required length."
  [short-str pad-loc pad-char min-len]

  (loop [short-str short-str pad-loc pad-loc pad-char pad-char min-len min-len]
    (if (>= (count short-str) min-len)
      short-str
      (let
        [new-str (if (= pad-loc "prefix")
                   (str pad-char short-str)
                   (if (= pad-loc "suffix")
                     (str short-str pad-char)))]
        (recur new-str pad-loc pad-char min-len)))))

(defn get-source-field-names "Returns the correct source field names from a dataset, accounting for data source."
  [& {:keys [data data-source]}]

  (cond
    ; If the data comes from QlikView / Qlik...
    (= data-source "qv")

      ; Remove the first character of the first field, which does not parse correctly.
      (assoc
        (first data)
        0
        (subs (first (first data)) 1))

    ; Return the first row of the data file.
    :else
      (first data)))

(defn scale-numeric "Scales a numeric number by a scaling factor."
  [& {:keys [field-name numeric-field-names scaling-factors maybe-numeric]}]

  ;(debug "scale numeric:")
  ;(? scaling-factors)
  ;(? (map type (nth scaling-factors 1)))
  ;(? (type maybe-numeric))
  ;(? maybe-numeric)
  ; Check if field-name is in numeric-field-names
  (vec-has-val numeric-field-names field-name)

  (? (/ maybe-numeric


     ; Get the scaling factor.
     (nth

      ; Get the scaling factors.
      (nth scaling-factors 1)

      ; Get the index of the field
      (.indexOf

       ; Get the field-names
       (nth scaling-factors 0)

       field-name))))

  ; Divide the numeric by the scaling factor
  (/ maybe-numeric


     ; Get the scaling factor.
     (nth

      ; Get the scaling factors.
      (nth scaling-factors 1)

      ; Get the index of the field
      (.indexOf

       ; Get the field-names
       (nth scaling-factors 0)

       field-name))))


(defn split-chars "Splits a string into its characters as a list."
  [string]

  (for [char-index (range 0 (.length string))]
    (subs string char-index (+ char-index 1))))

(defn make-dummy-values "Converts a categofic field value into a list of binary values."
  [& {:keys [field-name distinct-values value]}]

  ; Define local values.
  (let
    ; Get the list of distinct values for the field.
    [field-values (nth
                     ; Get the values  list row.
                     (nth distinct-values 1)

                       ; Look up the field's position in the header row.
                       (.indexOf (nth distinct-values 0) field-name))
     value-count (count field-values)
     value-index (.indexOf field-values value)]

    ;(? field-values)
    ;(? value)
    ;(? value-count)
    ;(? value-index)
    (concat
     (repeat (max value-index 0) "0")
     (list (if (>= value-index 0) "1" "0"))
     (repeat (- value-count (max value-index 0) 1) "0")
     )))

(defn str-vec "Maps all elements of a vector to strings."
  [source-vec]

  (mapv str source-vec))

(defn describe-product-dataset "Returns a map, describing the processed input count, output count, and observation count of a dataset."
  [& {:keys [numeric-field-names categoric-field-names distinct-values observation-data input-field-names output-field-names]}]

  (do
    (info "Describing product dataset...")
    (debug "describe-product-dataset arguments (selected):")
    (debug (str "input-field-names:\n" (str-col input-field-names)))
    (debug (str "output-field-names:\n" (str-col output-field-names)))
    (debug (str "categoric-field-names:\n" (str-col categoric-field-names)))
    (debug (str "distinct-values headers:\n" (str-col (first distinct-values))))

    (let
      [observation-count (count observation-data)
       distinct-values-headers (nth distinct-values 0)
       distinct-values-lists (nth distinct-values 1)
       input-field-count (+ 1 (reduce +
                                 (for [field-name input-field-names]
                                   (do (? field-name)
                                   (if (vec-has-val categoric-field-names field-name)
                                     (count
                                      (nth
                                       distinct-values-lists
                                       (.indexOf
                                        distinct-values-headers
                                        field-name)))
                                     1)))))
       output-field-count (reduce +
                                 (for [field-name output-field-names]
                                   (do (? field-name)
                                   (if (vec-has-val categoric-field-names field-name)
                                     (count
                                      (nth
                                       distinct-values-lists
                                       (.indexOf
                                        distinct-values-headers
                                        field-name)))
                                     1))))]
      (do
        (info "Finished describing product dataset.")
        (hash-map
         :observation-count observation-count
         :input-field-count input-field-count
         :output-field-count output-field-count)))))

(defn  describe-processed-field-names "Describes field names after processing."
  [& {:keys [source-field-names input-field-names output-field-names numeric-field-names categoric-field-names distinct-values]}]

  (do
    (debug "describe-processed-field-names:")
    (? input-field-names)
    (? output-field-names)
    (? (vec (flatten (conj input-field-names "bias" output-field-names))))

    (vec (flatten (map

       ; Field name mapping function.
       (fn [field-name]
         (cond

           ; Bias is unchanged.
           (= field-name "bias")
           "bias"

           ; Numeric field names are unchanged.
           (vec-has-val numeric-field-names field-name)
           field-name

           ; Categoric fields are given a column per distinct value.
           (vec-has-val categoric-field-names field-name)
           (flatten (mapv

              #(str field-name "__" %)

              (nth
                 (nth distinct-values 1)

                 (.indexOf (nth distinct-values 0) field-name))))))

       ; Field processing order.
       (vec (flatten (conj input-field-names "bias" output-field-names))))))))

; Needs to contain all information required for consistent incremental processing results, when combined with distinct-values and scaling-factors.
(defn describe-processing-parameters "Creates a map of processing options.."
  [& {:keys [numeric-field-names categoric-field-names input-field-names output-field-names dataset-name source-field-names distinct-values]}]

  (do
    (info "Describing processing parameters...")
    (let [result {:numeric-field-names numeric-field-names
                 :categoric-field-names categoric-field-names
                 :input-field-names input-field-names
                 :output-field-names output-field-names
                 :dataset-name dataset-name
                 :canonical-source-field-names source-field-names
                 :processed-field-names (describe-processed-field-names
                                         :source-field-names source-field-names
                                         :input-field-names input-field-names
                                         :output-field-names output-field-names
                                         :numeric-field-names numeric-field-names
                                         :categoric-field-names categoric-field-names
                                         :distinct-values distinct-values)}]
      (do
        (info "Finished describing processing parameters.")
        result))))

(defn create-csv "Returns of a vector of vectors of strings from a data structure."
  [& {:keys [data-struct style]}]
  (cond
   (= style "columnar")
     (cond
      (= (right (str (type data-struct)) 3) "Map")
       [(vec (map name (keys data-struct)))
         (vec (map str (vals data-struct)))])))


(defn ann-process-element
  [& {:keys [element-index unprocessed-element source-field-names numeric-field-names distinct-values scaling-factors]}]
  (let
    [field-name (nth source-field-names element-index)]
    (if (vec-has-val numeric-field-names field-name)
      (vector (str (scale-numeric
                    :field-name field-name
                    :numeric-field-names numeric-field-names
                    :scaling-factors scaling-factors
                    :maybe-numeric unprocessed-element)))
      (vec (make-dummy-values
            :field-name field-name
            :distinct-values distinct-values
            :value unprocessed-element)))))

(defn get-rearrangement-vector "Returns a vector with the positions of old elements in a new order."
  [& {:keys [source-vec target-order-vec]}]

  ; Map the elements in the target order vector.
  (map

   ; Map the elements, putting the old element indices in a new order.
   (into {} (map-indexed
               #(vector %2 %1)
               source-vec))

   ; Vector that will be mapped.
   target-order-vec))

(defn process-row "Processes a row of data."
  [element-processing-function unproc-vec]

  ; Map the row and convert it into a vector.
  (vec (map-indexed element-processing-function unproc-vec)))

(defn mapvv "Applies a function to each element in a vector of vectors."
  [element-processing-function vec-of-vecs]

  ; Map each row.
  (mapv

     ; Apply process-row to the vector, passing in f and the
     (partial process-row element-processing-function)

     ; Vector of vectors being mapped.
     vec-of-vecs))

(defn get-or-default "Performs get but returns default if get returns nil."
  [default first-arg second-arg]
  ;(debug "get-or-default")
  ;(? default)
  ;(? first-arg)
  ;(? second-arg)
  ;(? (get first-arg second-arg))
  (if-nil (get first-arg second-arg) default))

(defn rearrange "Rearranges a vector according to a new positional vector.  Nil elements are replaced with default-val."
  [& {:keys [position-vec data default-val]}]

  ; Map the vector of vectors.
  (mapv

   ; Map each row
   ; Convert to vector the flattened result of mapping the row.
   #(vec (flatten (mapv

                     ; Retrieve the new element for this position, or the default-val if no new position exists.
                     (partial get-or-default default-val %)

                   ; Row being mapped, containing the old p.
                   position-vec)))

   ; Vector of vectors being mapped.
   data))

(defn process-for-ann "Statistically processees data for analysis by a neural network."
  [& {:keys [unprocessed-data source-field-names input-field-names output-field-names element-processing-function bias]}]

  ; Define the new positions that elements will have.
  (let [rearrangement-vector (get-rearrangement-vector
                                :source-vec source-field-names

                                ; Define the ideal new arrangement of elements, with nil as a placeholder for the bias.
                                :target-order-vec (concat input-field-names [nil] output-field-names))]
    ;(? rearrangement-vector)
    ;(? (mapvv element-processing-function unprocessed-data))
    (rearrange
       :position-vec rearrangement-vector
       :data (mapvv element-processing-function unprocessed-data)
       :default-val bias)))

(defn generate-processed-data
  [& {:keys [unprocessed-data source-field-names input-field-names output-field-names numeric-field-names scaling-factors distinct-values ;element-processing-function
             ]}]

  (info-wrap "Generating processed data..."
             (process-for-ann
              :unprocessed-data unprocessed-data
              :source-field-names source-field-names
              :input-field-names input-field-names
              :output-field-names output-field-names
              :element-processing-function #(ann-process-element
                                             :element-index %1
                                             :unprocessed-element %2
                                             :source-field-names source-field-names
                                             :numeric-field-names numeric-field-names
                                             :distinct-values distinct-values
                                             :scaling-factors scaling-factors)
              :bias (str bias-const))
             "Finished generating processed data."))

(defn load-observation-data "Loads observation data, parsing fields not listed as categoric into Double."
  [& {:keys [categoric-field-names source-field-names source-data]}]

  (debug "load-observation-data:")
  (? categoric-field-names)
  (? source-field-names)
  ;(? (rest source-data))

  ; Map numeric fields to doubles.
  (map

   ; Define the function to map numeric fields to doubles.
   #(map-indexed

     ; Function to convert numerics in vector to doubles.
     (fn [field-index field-value]

       (do
         ;(debug "Set of categoric-field-names" (set categoric-field-names))
         ;(debug (str "Set of " field-index "th source-field-names")
         ;                  (set [ (nth source-field-names field-index)]))
;         (? categoric-field-names)
;         (? (nth source-field-names field-index))
;         (? (vec-has-val categoric-field-names (nth source-field-names field-index)))

         ; If the field index corresponds to a field that is not in the categoric fields list
         (if
           (not (vec-has-val categoric-field-names (nth source-field-names field-index)))

           ; Return the number as a double.
           (parse-double field-value)

           ; Leave the string unchanged.
           field-value)))

     ; Vector supplied by map.
     %)

   ; Supply the actual observation data.
   (rest source-data)))


; File writing functions.

(defn write-support-file "Serializes, encrypts, and writes to file a data structure."
  [& {:keys [support-data folder-path dataset-name data-name]}]

  (do
    ; Log debugging data.
    (debug "write-support-file arguments (selected):")
    (? folder-path)
    (? dataset-name)
    (? data-name)

    (let
      ; Define the full file path.
      [file-path (clojure.java.io/file folder-path (str dataset-name "_" data-name ".nef"))]
      (? file-path)

      ; Create a temporary variable for opening a file.
      (with-open

        ; Define the input stream for byte data
        [w (clojure.java.io/output-stream file-path)]
        (.write w (nippy/freeze support-data {:password [:cached support-file-password]}))))))

(defn load-support-file "Loads, decryptes, and deserializes a data structure from a file."
  [& {:keys [folder-path dataset-name data-name]}]

  (do
    ; Log debugging data.
    (debug "load-support-file arguments:")
    (? folder-path)
    (? dataset-name)
    (? data-name)

    (let
      ; Define the file path.
      [file-path

         ; Construct the full file path.
         (clojure.java.io/file folder-path

                               ; Construct the file name.
                               (str dataset-name "_" data-name ".nef"))]
      (? file-path)

      ; Generate the data structure.
      (nippy/thaw

       ; Convert the file data into a byte array.
       (IOUtils/toByteArray

        ; Open an input stream to the file path.
        (clojure.java.io/input-stream file-path))

    ; Supply password for encryption.
    {:password [:cached support-file-password]}))))

(defn load-scaling-factors "Loads the scaling factors support file."
  [& {:keys [folder-path dataset-name]}]

  (info-wrap "Loading scaling factors..."
             (load-support-file
              :folder-path folder-path
              :dataset-name dataset-name
              :data-name "scaling_factors")
             "Finished loading scaling factors."))

(defn load-distinct-values "Loads the distinct values support file."
  [& {:keys [folder-path dataset-name]}]

  (info-wrap "Loading distinct values..."
             (load-support-file
              :folder-path folder-path
              :dataset-name dataset-name
              :data-name "distinct_values")
             "Finished loading distinct values."))

(defn load-processing-parameters "Loads the support file containing processing parameters."
  [& {:keys [folder-path dataset-name]}]

  (do
    (info "Loading processing parameters...")
    (let [processing-parameters (load-support-file
                                 :folder-path folder-path
                                 :dataset-name dataset-name
                                 :data-name "processing_parameters")]
      (do
        (info "Finished loading processing parameters.")
        processing-parameters))))

(defn write-product-file "Parses and write to file a vector of vector of strings."
  [& {:keys [table-data folder-path dataset-name table-name file-path]}]

  (do
    ; Log debugging data.
    (debug "write-product-file arguments (selected):")
    (? folder-path)
    (? dataset-name)
    (? table-name)

    ; Write the data to the file path.
    (spit

       ; Use the file path. Otherwise, build a file path out of the folder, the dataset name, and the table name.
       (if-nil file-path (str folder-path "/" dataset-name "_" table-name ".csv"))

       ; Parse the data.
       (csv/write-csv table-data))))

(defn revert-prediction-data "Reverts predictions by reversing scaling."
  [& {:keys [predictions field-names scaling-factors]}]

  (do
    (info "Reverting predictions...")
    (debug "revert-predictions:")
    (? field-names)
    (let
      [scaled-field-names (nth scaling-factors 0)
       factors (nth scaling-factors 1)
       result (mapv

                 (fn [row]

                   (vec (map-indexed

                         (fn [idx elem]

                           (? elem)
                           (? idx)
                           (let [field-name (nth field-names idx)]

                             (? field-name)
                             ; If the field name is in the scaling-factors field names...
                             (if (vec-has-val scaled-field-names field-name)

                               ; Divide the element by the corresponding scaling factor.
                               (* elem
                                  (nth

                                   ; Get the scaling factor.
                                   factors

                                   ; Get the index of the field name in scaling-factors.
                                   (.indexOf scaled-field-names field-name)))

                               ; Return the element unchanged.
                               elem)))

                         row)))

                 predictions)]

      (do
         (info "Finished reverting predictions.")
         result))))

(defn revert-sensitivity-data "Reverts sensitivity data by reversing scaling."
  [& {:keys [sensitivity processed-field-names numeric-field-names scaling-factors]}]

  (do
    (info "Reverting sensitivity data...")
    (debug "revert-sensitivity-data:")
    (? sensitivity)
    (? processed-field-names)
    (? numeric-field-names)

    (let
      [factors (nth scaling-factors 1)
       scaled-field-names (nth scaling-factors 0)
       inputs (get-processed-input-field-names processed-field-names)
       outputs (get-processed-output-field-names processed-field-names)
       result (vec (map-indexed

                     ; Row-mapping function
                     (fn [input-idx row]
                       (let [input-field-name (nth inputs input-idx)

                             ; If the field name is a numeric field name...
                             input-scaling-factor (if (vec-has-val numeric-field-names input-field-name)

                                                      ; Get the input scaling factor.
                                                      (nth factors (.indexOf scaled-field-names input-field-name))

                                                      ; Otherwise, do not scale based on input.
                                                      1.0)]

                         (vec (map-indexed

                               (fn [output-idx elem]

                                 (let [output-field-name (nth outputs output-idx)

                                                             ; If the field name is a numeric field name...
                                       output-scaling-factor (if (vec-has-val numeric-field-names output-field-name)

                                                                 ; Get the output scaling factor.
                                                                 (nth factors (.indexOf scaled-field-names output-field-name))

                                                                 ; Otherwise, do not scale based on output.
                                                                 1.0)]

                                   (/
                                     ; Scale based on output.
                                     (* elem output-scaling-factor)

                                     input-scaling-factor)))

                               row))))

                     sensitivity))]

      (do
        (? inputs)
        (? outputs)
        (info "Finished reverting sensitivity data.")
        result))))

(defn generate-reverted-sensitivity-table "Generates a reverted sensitivity table with headers and labels for consumption."
  [& {:keys [reverted-sensitivity proc-inputs proc-outputs]}]

  (do
    (info "Generating reverted sensitivity table.")

    (let [output-count (count proc-outputs)
          input-count (count proc-inputs)

          ; Append "Sensitivity" to each output name.
          sensitivity-output-names (mapv #(str % " Sensitivity") proc-outputs)


          ; Generate the header row for the product file.
          header (into
                  ["Processed Input"]
                  sensitivity-output-names)

          ; Construct the product
          product (into [header]

                         ; Convert "for" row sequence to vector
                         (vec

                            ; Loop through each input, each of which corresponds to a row. The last row, for the bias, is skipped.
                            (for [input-idx (range input-count)]

                                ; Define the current senstivity row.
                                (let [sense-row (nth reverted-sensitivity input-idx)
                                      input-name (nth proc-inputs input-idx)]

                                  ; Convert "for" column sequence to vector.
                                  (vec

                                   ; Loop through each column in the product, one for each output and one for processed input name.
                                   (for [col-idx (range (+ 1 output-count))]

                                     ; If this is the first column...
                                     (if (= 0 col-idx)

                                       ; Return the name of the input.
                                       input-name

                                       ; Else, return the sensitivity of the current input to the current output.
                                       (str (nth sense-row (- col-idx 1))))))))))]

      (do
        (info "Finished generating reverted senstivity table.")

        ; Finally return the value.
        product))))

(defn generate-reverted-predictions-table "Generates a reverted prediction table with headers for consumption."
  [& {:keys [reverted-predictions proc-outputs]}]

  (do
     (info "Generating reverted predictions table.")

          ; Prefix each field with "Predicted."
     (let [header (mapv #(str "Predicted " %) proc-outputs)

           product (into [header]

                         (stringify-table reverted-predictions))]

       (do
         (info "Finished generating reverted predictions table.")

         ; Finally return the value.
         product))))










; Source file and product file functions.

(defn load-statistical-data
  [& {:keys [source-file-path]}]

  (debug "load-statistical-data:")
  (? source-file-path)

  ; Map each row into a vector.
  (mapv

   ; Row-mapping function.
   (fn [row]

     ; Map each element into a vector.
     (mapv

      ; Parse each element to a double.
      #(parse-double %)

      ; Row being processed.
      row))

   ; Load and parse the file as a csv.
   (csv/parse-csv (slurp source-file-path))))

(defn load-prediction-data "Loads statistical prediction data."
  [& {:keys [file-path]}]

  (info-wrap "Loading prediction data..."
             (load-statistical-data
              :source-file-path file-path)
             "Finished loading prediction data."))

(defn load-sensitivity-data "Loads statistical data from ANN."
  [& {:keys [file-path]}]

  (info-wrap "Loading sensitivity data..."
             (load-statistical-data
              :source-file-path file-path)
             "Finished loading data."))

(defn validate-initial-fields "Validates field data in a file compared to metadata."
    [& {:keys [source-field-names categoric-field-names output-field-names]}]

    ; Check for invalid field data.
    (let
      [not-found-categories (clojure.set/difference
                             (set categoric-field-names)
                             (set source-field-names))
       not-found-categories-count (count not-found-categories)
       not-found-outputs (clojure.set/difference
                           (set output-field-names)
                           (set source-field-names))
       not-found-outputs-count (count not-found-outputs)]

      ; If there are any category fields not found in the source data...
      (if (> not-found-categories-count 0)

        ; Warn the list the missing fields.
        (error (str "Missing category fields: " (clojure.string/join ", " not-found-categories))))

      ; If there are any output fields not found in the source data...
      (if (> not-found-outputs-count 0)

        ; Error the list of missing fields.
        (error (str "Missing output fields: " (clojure.string/join ", " not-found-outputs))))))

(defn validate-incremental-fields "Valdiates field data in a file compared to metadata."
  [& {:keys [source-field-names expected-field-names]}]

  ; Check for field name differences.
  (let
    [extra-field-names (clojure.set/difference
                        (set source-field-names)
                        (set expected-field-names))
     extra-field-count (count extra-field-names)
     missing-field-names (clojure.set/difference
                          (set expected-field-names)
                          (set source-field-names))
     missing-field-count (count missing-field-names)]

    ; If there any extra fields...
    (if (> extra-field-count 0)

      ; Warn and list the extra fields found.
      (error (str "Extra fields found: " (clojure.string/join ", " extra-field-names))))

    ; If there are any missing fields...
    (if (> missing-field-count 0)

      ; Warn and list the missing fields found.
      (error (str "Missing fields: " (clojure.string/join ", " missing-field-names))))))


(defn load-initial-data
  [& {:keys [dataset-source-file-path categoric-field-names maybe-output-field-names data-source]}]

  ; Load initial data.
  (let

    ; Load and parse file.
    [initial-data-source (csv/parse-csv (slurp dataset-source-file-path))]
    ;(? initial-data-source)

    ; Get source field names.
    (def source-field-names (get-source-field-names :data initial-data-source :data-source data-source))
    (? source-field-names)

    ; Define the numeric fields as the fields not included in categoric-fields.
    (def numeric-field-names
      (vec
       (sort
         (set/difference
          (set source-field-names)
          (set categoric-field-names)))))
    (? numeric-field-names)

    ; Define the output field names from the argument, if supplied, or assume it from the last field name.
    (def output-field-names (if (> (count maybe-output-field-names) 0)
                              (vec (sort maybe-output-field-names))
                              [(last source-field-names)]))
    (? output-field-names)

    ; Define input fields as the fields not included in output-fields.
    (def input-field-names
      (vec
       (sort
         (set/difference
          (set source-field-names)
          (set output-field-names)))))
    (? input-field-names)

    (debug (str "source-field-names:\n" (str-col source-field-names)))
    (debug (str "numeric-field-names:\n" (str-col numeric-field-names)))
    (debug (str "categoric-field-names:\n" (str-col categoric-field-names)))
    (debug (str "input-field-names:\n" (str-col input-field-names)))
    (debug (str "output-field-names:\n" (str-col output-field-names)))

    ; Check for invalid field data.
    (let
      [not-found-categories (clojure.set/difference
                             (set categoric-field-names)
                             (set source-field-names))
       not-found-categories-count (count not-found-categories)
       not-found-outputs (clojure.set/difference
                           (set output-field-names)
                           (set source-field-names))
       not-found-outputs-count (count not-found-outputs)]

      ; If there are any category fields not found in the source data...
      (if (> not-found-categories-count 0)

        ; Warn the list the missing fields.
        (error (str "Missing category fields: " (clojure.string/join ", " not-found-categories))))

      ; If there are any output fields not found in the source data...
      (if (> not-found-outputs-count 0)

        ; Error the list of missing fields.
        (error (str "Missing output fields: " (clojure.string/join ", " not-found-outputs)))))

    ; Define observation data.
    (def initial-observation-data "initial data, excluding headers."

      (load-observation-data
       :categoric-field-names categoric-field-names
       :source-field-names source-field-names
       :source-data initial-data-source)
      )
    ;(? initial-observation-data)
    ))

(defn load-full-observation-data-source "Loads a CSV into memory and parses it."
  [& {:keys [file-path]}]

  (do
    (info "Loading observation data...")

    (let [data (csv/parse-csv (slurp file-path))]

      (do
        (info "Finished loading observation data.")
        data))))

(defn load-incremental-data "Loads data for incremental processing."
  [& {:keys [dataset-source-file-path expected-field-names categoric-field-names data-source]}]

  (let

    ; Load and parse file.
    [incremental-data-source (csv/parse-csv (slurp dataset-source-file-path))]

    ; Get source field names.
    (def source-field-names (get-source-field-names :data incremental-data-source :data-source data-source))
    (? source-field-names)
    (? expected-field-names)
    (? (= source-field-names expected-field-names))

    ; Check for field name differences.
    (let
      [extra-field-names (clojure.set/difference
                          (set source-field-names)
                          (set expected-field-names))
       extra-field-count (count extra-field-names)
       missing-field-names (clojure.set/difference
                          (set expected-field-names)
                          (set source-field-names))
       missing-field-count (count missing-field-names)]

      ; If there any extra fields...
      (if (> extra-field-count 0)

        ; Warn and list the extra fields found.
        (warn (str "Extra fields found: " (clojure.string/join ", " extra-field-names))))

      ; If there are any missing fields...
      (if (> missing-field-count 0)

        ; Warn and list the missing fields found.
        (warn (str "Missing fields: " (clojure.string/join ", " missing-field-names)))))

    ; Define observation data.
    (def incremental-observation-data "Incremental data, excluding headers."
      (load-observation-data
       :categoric-field-names categoric-field-names
       :source-field-names source-field-names
       :source-data incremental-data-source))))

(defn define-incremental-processing-parameters "Defines processing symboles from processing parameters."
  [& {:keys [processing-parameters]}]

  (def canonical-source-field-names (:canonical-source-field-names processing-parameters))
  (def input-field-names (:input-field-names processing-parameters))
  (def output-field-names (:output-field-names processing-parameters))
  (def numeric-field-names (:numeric-field-names processing-parameters))
  (def categoric-field-names (:categoric-field-names processing-parameters)))

(defn calculate-scaling-factors "Calculates scaling factors from a vector of vectors."
  [& {:keys [numeric-field-names source-field-names initial-observation-data]}]

  (do

    (info "Calculating scaling factors...")

      (let [scaling-factors [numeric-field-names

                           ; Create a vector by looping through each numeric field name.
                           (mapv

                              ; Replace all zero values with one.
                              (fn [factor] (if (= factor 0.0) 1.0 factor))

                              ; For each numeric field name
                              (pmap

                               ; Find the index of the numeric field.
                               (fn [numeric-field-name]

                                 (let [field-index (.indexOf source-field-names numeric-field-name)]

                                   ; Walk through the source data to find the largest absolute value.
                                   (walk/walk

                                      ; Get the nth field from each row.
                                      (fn [val] (nth val field-index))

                                      ; Find the largest absolute value in the resulting vector.  This is the applied function.
                                      max-abs-vec

                                      ; The data subject to the walk.
                                      initial-observation-data)))

                               numeric-field-names))]]

      (do
        (info "Finished calculating scaling factors.")
        scaling-factors))))

(defn calculate-distinct-values "Calculates distinct values from a vector of vectors."
  [& {:keys [categoric-field-names source-field-names initial-observation-data]}]
  (do
    (info "Calculating distinct values...")
    (let [distinct-values (sort-by-field-and-field-data
                              [categoric-field-names

                              ; Convert to vector
                              (vec

                                 ; For each categoric field name
                                 (for [categoric-field-name categoric-field-names

                                     ; Find the index of the categoric field name.
                                     :let [field-index (.indexOf source-field-names categoric-field-name)]]

                                     ; Walk throught the source data and find the unique values.
                                     (walk/walk

                                      ; Get the nth field from each row.
                                      #(nth % field-index)

                                      ; Find the unique values.
                                      distinct

                                      ; The data subject to the walk.
                                      initial-observation-data)))])]
      (do
        (info "Finished calculating distinct values.")
        distinct-values))))










; Mode functions.

(defn initial "Processes a dataset, generating metadata for consistent processing of dependent datasets."
  [& {:keys [initial-dataset-file-path metadata-folder-path initial-product-file-path categoric-field-names maybe-output-field-names dataset-name data-source]}]
  (do
    (info "Initial processing...")
    (debug "initial arguments:")
    (? initial-dataset-file-path)
    (? dataset-name)
    (? metadata-folder-path)
    (? initial-product-file-path)
    (? categoric-field-names)

    (let [
          ; Load data from file.
          initial-data-source (future (csv/parse-csv (slurp initial-dataset-file-path)))

          ; Determine file headers.
          source-field-names (future (get-source-field-names
                                      :data @initial-data-source
                                      :data-source data-source))

          ; Determine numeric field names.
          numeric-field-names (vec
                                       (sort
                                         (set/difference
                                          (set @source-field-names)
                                          (set categoric-field-names))))

          ; Determine output field names.
          output-field-names (if (> (count maybe-output-field-names) 0)
                                         (vec (sort maybe-output-field-names))
                                         [(last @source-field-names)])

          ; Determine input field names.
          input-field-names (vec
                                     (sort
                                       (set/difference
                                        (set @source-field-names)
                                        (set output-field-names))))

          ; Determine initial observation data.
          initial-observation-data (future (load-observation-data
                                           :categoric-field-names categoric-field-names
                                           :source-field-names @source-field-names
                                           :source-data @initial-data-source))

          ; Find scaling factors in numeric fieds.
          scaling-factors (future (calculate-scaling-factors
                                   :numeric-field-names numeric-field-names
                                   :source-field-names @source-field-names
                                   :initial-observation-data @initial-observation-data))

          ; Find distinct values in categoric fields.
          distinct-values (future (calculate-distinct-values
                                   :categoric-field-names categoric-field-names
                                   :source-field-names @source-field-names
                                   :initial-observation-data @initial-observation-data))

          ; Describe the product dataset.
          product-dataset-description (future (describe-product-dataset
                                               :numeric-field-names numeric-field-names
                                               :categoric-field-names categoric-field-names
                                               :distinct-values @distinct-values
                                               :observation-data @initial-observation-data
                                               :input-field-names input-field-names
                                               :output-field-names output-field-names))

          ; Describe processing parameters.
          processing-parameters (future (describe-processing-parameters
                                         :numeric-field-names numeric-field-names
                                         :categoric-field-names categoric-field-names
                                         :input-field-names input-field-names
                                         :output-field-names output-field-names
                                         :dataset-name dataset-name
                                         :source-field-names @source-field-names
                                         :distinct-values @distinct-values))

          ; Create processed data
          processed-data (future (generate-processed-data
                                 :unprocessed-data @initial-observation-data
                                 :source-field-names @source-field-names
                                 :input-field-names input-field-names
                                 :output-field-names output-field-names
                                 :numeric-field-names numeric-field-names
                                 :scaling-factors @scaling-factors
                                 :distinct-values @distinct-values))]

      (info-wrap "Validating fields...";
                 (validate-initial-fields
                  :source-field-names @source-field-names
                  :categoric-field-names categoric-field-names
                  :output-field-names output-field-names)
                 "Finished validating fields.")

      ;(debug (str "source-field-names:\n" (str-col @source-field-names)))
      ;(debug (str "numeric-field-names:\n" (str-col numeric-field-names)))
      ;(debug (str "categoric-field-names:\n" (str-col categoric-field-names)))
      (? @scaling-factors)
      (? @distinct-values)
      (? @processing-parameters)

      ; Store scaling factors.
      (info-wrap "Storing scaling factors..."
                 (write-support-file
                  :support-data @scaling-factors
                  :data-name "scaling_factors"
                  :dataset-name dataset-name
                  :folder-path metadata-folder-path)
                 "Scaling factors stored.")

      ; Store distinct values
      (info-wrap "Storing distinct values..."
                  (write-support-file
                   :support-data @distinct-values
                   :data-name "distinct_values"
                   :dataset-name dataset-name
                   :folder-path metadata-folder-path)
                  "Finished storing distinct values.")

      ; Store product dataset description.
      (info-wrap "Storing dataset description..."
                  (write-product-file
                   :table-data (stringify-table (create-csv
                                                 :data-struct @product-dataset-description
                                                 :style "columnar"))
                   :table-name "dataset_description"
                   :dataset-name dataset-name
                   :folder-path metadata-folder-path)
                  "Finished storing dataset description.")

      ; Store processing parameters.
      (info-wrap "Storing processing parameters..."
                  (write-support-file
                   :support-data @processing-parameters
                   :data-name "processing_parameters"
                   :dataset-name dataset-name
                   :folder-path metadata-folder-path)
                  "Finished storing processing parameters.")

      ; Write processed data file
      (info-wrap "Writing processed data..."
                  (write-product-file
                   :table-data @processed-data
                   :table-name "processed"
                   :dataset-name dataset-name
                   :folder-path metadata-folder-path)
                  "Finished writing processed data."))

    ; Prevents one-minute wait for threads, created by future usage, to shut down.
    (shutdown-agents)

    (info "Finished initial processing.")))

(defn incremental "Processes a dataset according to metadata generated from earlier processing on initial data."
  [& {:keys [incremental-dataset-file-path metadata-folder-path incremental-product-file-path cli-dataset-name data-source]}]

  (do
    (info "Incremental processing...")
    (debug "incremental arguments:")
    (? incremental-dataset-file-path)
    (? metadata-folder-path)
    (? incremental-product-file-path)
    (? cli-dataset-name)

    ; Perform set up and field validation.
    (let [
          ; Define the dataset name.
          dataset-name (define-dataset-name
                        :cli-dataset-name cli-dataset-name
                        :metadata-folder-path metadata-folder-path)

          ; Retrieve processing parameters.
          processing-parameters (load-processing-parameters
                                 :folder-path metadata-folder-path
                                 :dataset-name dataset-name)

          ; Define parameters.
          canonical-source-field-names (:canonical-source-field-names processing-parameters)
          input-field-names (:input-field-names processing-parameters)
          output-field-names (:output-field-names processing-parameters)
          numeric-field-names (:numeric-field-names processing-parameters)
          categoric-field-names (:categoric-field-names processing-parameters)

          ; Get source field names.
          source-field-names (with-open [file (clojure.java.io/reader incremental-dataset-file-path)]
                                (get-source-field-names
                                :data (csv/parse-csv file)
                                :data-source data-source))]

      (do
        (validate-incremental-fields
         :source-field-names source-field-names
         :expected-field-names canonical-source-field-names)

        ; Perform processing.
        (let [
              ; Load incremental data.
              incremental-data-source (future (load-full-observation-data-source :file-path incremental-dataset-file-path))

              ; Define observation data
              incremental-observation-data (future (load-observation-data
                                                    :categoric-field-names categoric-field-names
                                                    :source-field-names source-field-names
                                                    :source-data @incremental-data-source))

              ; Load scaling table.
              scaling-factors (future (load-scaling-factors
                                       :folder-path metadata-folder-path
                                       :dataset-name dataset-name))

              ; Load distinct value table.
              distinct-values (future (load-distinct-values
                                       :folder-path metadata-folder-path
                                       :dataset-name dataset-name))

              ; Describe the product dataset
              product-dataset-description (future (describe-product-dataset
                                                   :numeric-field-names numeric-field-names
                                                   :categoric-field-names categoric-field-names
                                                   :distinct-values @distinct-values
                                                   :observation-data @incremental-observation-data
                                                   :input-field-names input-field-names
                                                   :output-field-names output-field-names))

              ; Create processed data
              processed-data (future (generate-processed-data
                                     :unprocessed-data @incremental-observation-data
                                     :source-field-names source-field-names
                                     :input-field-names input-field-names
                                     :output-field-names output-field-names
                                     :numeric-field-names numeric-field-names
                                     :scaling-factors @scaling-factors
                                     :distinct-values @distinct-values))]

          (? processing-parameters)
          (? @distinct-values)

          ; Store dataset description.
          (info-wrap "Storing dataset description..."
                     (write-product-file
                     :table-data (stringify-table (create-csv
                                                   :data-struct @product-dataset-description
                                                   :style "columnar"))
                     :table-name "dataset_description"
                     :dataset-name (rfs/base-name
                                      incremental-dataset-file-path
                                      true)
                     :folder-path metadata-folder-path)
                     "Finished storing dataset description.")

          ; Store processed data.
          (info-wrap "Writing processed data..."
                     (write-product-file
                     :table-data @processed-data
                     :table-name "processed"
                     :dataset-name dataset-name
                     :folder-path metadata-folder-path
                     :file-path incremental-product-file-path)
                     "Finished writing processed data."))))

    ; Prevents one-minute wait for threads, created by future usage, to shut down.
    (shutdown-agents)

    (info "Finished incremental processing.")))

(defn revert-predictions "Reverts processing on a prediction dataset resulting from ANN training so that numbers are scaled normally and fields labeled."
  [& {:keys [predictions-dataset-file-path metadata-folder-path predictions-product-file-path cli-dataset-name]}]

  (do
    (info "Reverting predictions...")


    (let [
          ; Define dataset name.
           dataset-name (define-dataset-name
                        :cli-dataset-name cli-dataset-name
                        :metadata-folder-path metadata-folder-path)

          ; Retrieve processing parameters.
          processing-parameters (load-processing-parameters
                                 :folder-path metadata-folder-path
                                 :dataset-name dataset-name)

          ; Define parameters.
          canonical-source-field-names (:canonical-source-field-names processing-parameters)
          input-field-names (:input-field-names processing-parameters)
          output-field-names (:output-field-names processing-parameters)
          numeric-field-names (:numeric-field-names processing-parameters)
          categoric-field-names (:categoric-field-names processing-parameters)
          processed-field-names (:processed-field-names processing-parameters)
          product-field-names (get-processed-output-field-names processed-field-names)

          ; Load prediction data.
          prediction-data (future (load-prediction-data
                                   :file-path predictions-dataset-file-path))

          ; Load scaling table.
          scaling-factors (future (load-scaling-factors
                                   :folder-path metadata-folder-path
                                   :dataset-name dataset-name))

          ; Create reverted data.
          reverted-data (future (revert-prediction-data
                                 :predictions @prediction-data
                                 :field-names product-field-names
                                 :scaling-factors @scaling-factors))


          reverted-predictions-table (future (generate-reverted-predictions-table
                                              :reverted-predictions @reverted-data
                                              :proc-outputs product-field-names))]

      (? processing-parameters)

      ; Store reverted data.
      (info-wrap "Writing reverted data..."
                  (write-product-file
                   :table-data @reverted-predictions-table
                   :table-name "reverted_predictions"
                   :dataset-name dataset-name
                   :folder-path metadata-folder-path
                   :file-path predictions-product-file-path)
                  "Finished writing reverted data."))

      ; Prevents one-minute wait for threads, created by future usage, to shut down.
      (shutdown-agents)

      (info "Finished reverting predictions.")))

(defn revert-sensitivity "Reverts the scaling in a sensitivity file."
  [& {:keys [sensitivity-dataset-file-path metadata-folder-path sensitivity-product-file-path cli-dataset-name]}]

  (do
    (info "Reverting sensitivity...")


    (let [
          ; Define dataset name.
           dataset-name (define-dataset-name
                        :cli-dataset-name cli-dataset-name
                        :metadata-folder-path metadata-folder-path)

          ; Retrieve processing parameters.
          processing-parameters (load-processing-parameters
                                 :folder-path metadata-folder-path
                                 :dataset-name dataset-name)

          ; Define parameters.
          canonical-source-field-names (:canonical-source-field-names processing-parameters)
          input-field-names (:input-field-names processing-parameters)
          output-field-names (:output-field-names processing-parameters)
          numeric-field-names (:numeric-field-names processing-parameters)
          categoric-field-names (:categoric-field-names processing-parameters)
          processed-field-names (:processed-field-names processing-parameters)
          processed-input-field-names (get-processed-input-field-names processed-field-names)
          processed-output-field-names (get-processed-output-field-names processed-field-names)

          ; Load sensitivity data.
          sensitivity-data (future (load-sensitivity-data
                                    :file-path sensitivity-dataset-file-path))

          ; Load scaling table.
          scaling-factors (future (load-scaling-factors
                                   :folder-path metadata-folder-path
                                   :dataset-name dataset-name))

          ; Create reverted data.
          reverted-data (future (revert-sensitivity-data
                                 :sensitivity @sensitivity-data
                                 :processed-field-names processed-field-names
                                 :numeric-field-names numeric-field-names
                                 :scaling-factors @scaling-factors))

          ; Format data for writing.
          sensitivity-product (future (generate-reverted-sensitivity-table
                                        :reverted-sensitivity @reverted-data
                                        :proc-inputs processed-input-field-names
                                        :proc-outputs processed-output-field-names))]

      ; Store reverted data.
      (info-wrap "Writing reverted data..."
                 (write-product-file
                  :table-data @sensitivity-product
                  :table-name "reverted_sensitivity"
                  :dataset-name dataset-name
                  :folder-path metadata-folder-path
                  :file-path sensitivity-product-file-path)
                 "Finished writing reverted data."))

    ; Prevents one-minute wait for threads, created by future usage, to shut down.
    (shutdown-agents)

    (info "Finished reverting sensitivity.")))









; Command-line option symbols

(def initial-dataset-cli-option ;"Command-line option describing how to parse the initial dataset file path."
  [nil "--initialdataset path/to/initial_data.csv" "File path to the initial dataset file."
      :parse-fn #(parse-cli-path %)
      ;:validate [#(some #{%} ["initial" "incremental"]) "Must be a valid mode."]
    ])

(def incremental-dataset-cli-option "Command-line option describing how to parse the incremental dataset file path."
  [nil "--incrementaldataset path/to/incremental_data.csv" "File path to the incremental dataset file."
      :parse-fn #(parse-cli-path %)
      ;:validate [#(some #{%} ["initial" "incremental"]) "Must be a valid mode."]
    ])

(def sensitivity-dataset-cli-option "Command-line option describing how to parse the sensitivity dataset file path."
  [nil "--sensitivitydataset path/to/sensitivity_data.csv" "File path to the sensitivity dataset file."
      :parse-fn #(parse-cli-path %)
      ;:validate [#(some #{%} ["initial" "incremental"]) "Must be a valid mode."]
    ])

(def prediction-dataset-cli-option "Command-line option decribing the location of the predictions dataset file path."
  [nil "--predictiondataset path/to/prediction_data.csv" "File path to the prediction dataset file."
     :parse-fn #(parse-cli-path %)])

(def metadata-folder-cli-option "Command-line option describing how to parse the metadata folder path."
  ["-mf" "--metadatafolder path/to/metadata/folder" "Folder path to metadata files."
     :default "."
     :default-desc "Folder containing executable."
     :parse-fn #(parse-cli-path %)])

(def initial-product-cli-option "Command-line option describing how to parse the product file path."
  [nil "--initialproduct path/to/initial_product.csv" "File path to the initial product file."
     :parse-fn #(parse-cli-path %)])

(def incremental-product-cli-option "Command-line option describing how to parse the product file path."
  [nil "--incrementalproduct path/to/incremental_product.csv" "File path to the incremental product file."
     :parse-fn #(parse-cli-path %)])

(def prediction-product-cli-option "Command-line option describing how to parse the product file path."
  [nil "--predictionproduct path/to/prediction_product.csv" "File path to the prediction product file."
     :parse-fn #(parse-cli-path %)])

(def sensitivity-product-cli-option "Command-line option describing how to parse the product file path."
  [nil "--sensitivityproduct path/to/incremental_product.csv" "File path to the sensitivity product file."
     :parse-fn #(parse-cli-path %)])

(def categoric-fields-cli-option "Command-line option describing which field names indicate categoric columns."
  [nil "--categories CSV-line-of-field-names" "List of field names with category data, in CSV format."
     :parse-fn #(parse-cli-field-list %)
     :default []])

(def output-fields-cli-option "Command-line option describing which field names indicate outputs."
  [nil "--outputs CSV-line-of-field-names" "List of field names with output data, in CSV format."
     :parse-fn #(parse-cli-field-list %)
     :default []
     :default-desc "The right-most field will be assumed the output."])

(def dataset-name-cli-option "Command-line option describing name to used for the dataset."
  [nil "--datasetname name_of_dataset"
     :parse_fn #(fix-dble-quotes %)
     :default nil
     :default-desc "Dataset name will be derived from dataset file base name."])

(def data-source-cli-option "Command-line option describing the source system for the dataset."
  [nil "--datasource name_of_source_system"
     :parse_fn #(fix-dble-quotes %)
     :default nil])

; Command-line parsing option sets.

(def startup-cli-options "Command-line options needed to determine application mode."
  [[nil "--mode MODE" "Processing mode."]])

(def initial-mode-cli-options "Command-line options needed to process initial data."
  [initial-dataset-cli-option
   metadata-folder-cli-option
   initial-product-cli-option
   categoric-fields-cli-option
   output-fields-cli-option
   dataset-name-cli-option
   data-source-cli-option])

(def incremental-mode-cli-options "Command-line options needed to process incremental data."
  [incremental-dataset-cli-option
   metadata-folder-cli-option
   incremental-product-cli-option
   dataset-name-cli-option
   data-source-cli-option])

(def revert-predictions-mode-cli-options "Command-line options needed to revert incremental data."
  [prediction-dataset-cli-option
   metadata-folder-cli-option
   prediction-product-cli-option
   dataset-name-cli-option])

(def revert-sensitivity-mode-cli-options "Command-line options needed to revert sensitivity data."
  [sensitivity-dataset-cli-option
   metadata-folder-cli-option
   sensitivity-product-cli-option
   dataset-name-cli-option])










; Main function.

(defn -main
  "Main command-line entry point for adp ADP."
  [& args]
  (do

    ; Begin startup.
    (info "Initializing the adp Engine Automated Dataset Processor...")

    ; Begin actual processing of command line options.
    (info "Welcome to the adp Engine Dataset Processor!")

    ; Begin reading command-line arguments
    (debug "Command-line arguments:" args)
    (def startup-options "Parsed command-line arguments."
      (clojure.tools.cli/parse-opts args startup-cli-options))

    ; Parse startup options from the command line.
    (info "Parsing startup options from the command line.")
    (def mode (:mode (:options startup-options)))
    (info "Mode:" mode)

    ; Parse mode options from the command line.
    (info "Parsing" mode "options from the command line.")
    (cond

     ; Parse command-line options for initial.
     (= mode "initial")
       (do
         (def parsed-initial-cli-options
           (:options (clojure.tools.cli/parse-opts args initial-mode-cli-options)))
         (? args)
         (? initial-mode-cli-options)
         (? parsed-initial-cli-options))

     ; Parse command-line options for incremental.
     (= mode "incremental")
       (do
         (def parsed-incremental-cli-options
           (:options (clojure.tools.cli/parse-opts args incremental-mode-cli-options)))
         (? parsed-incremental-cli-options))

    ; Parse command-line options for revert predictions.
    (= mode "revertpredictions")
       (do
         (def parsed-revert-predictions-cli-options
           (:options (clojure.tools.cli/parse-opts args revert-predictions-mode-cli-options)))
         (? parsed-revert-predictions-cli-options))

    (= mode "revertsensitivity")
      (do
        (def parsed-revert-sensitivity-cli-options
          (:options (clojure.tools.cli/parse-opts args revert-sensitivity-mode-cli-options)))
        (? parsed-revert-sensitivity-cli-options)))

    ; Enter into the appropriate subroutine.
    (cond
     (= mode "initial")
       (initial
          :initial-dataset-file-path (:initialdataset parsed-initial-cli-options)
          :metadata-folder-path (if-nil (:metadatafolder parsed-initial-cli-options)
                                        (rfs/parent
                                         (:initialdataset parsed-initial-cli-options)))
          :initial-product-file-path (if-nil  (:initialproduct parsed-initial-cli-options)

                                              ; Just added _processed onto the end of the dataset name if no ouput file name is given.
                                              (str
                                                 (rfs/base-name
                                                    (:initialdataset parsed-initial-cli-options)
                                                    true)
                                                 "_processed.csv"))
          :categoric-field-names (:categories parsed-initial-cli-options)
          :maybe-output-field-names (:outputs parsed-initial-cli-options)
          :dataset-name (if-nil (:datasetname parsed-initial-cli-options)
                          (rfs/base-name
                           (:initialdataset parsed-initial-cli-options)
                           true))
          :data-source (:datasource parsed-initial-cli-options))
     (= mode "incremental")
       (incremental
          :incremental-dataset-file-path (:incrementaldataset parsed-incremental-cli-options)
          :metadata-folder-path (if-nil (:metadatafolder parsed-incremental-cli-options)
                                        (rfs/parent
                                         (:incrementaldataset parsed-incremental-cli-options)))
          :incremental-product-file-path (if-nil (:incrementalproduct parsed-incremental-cli-options)

                                                 ; Just add _processed onto the end of the file base name if no product file name is given.
                                                 (clojure.java.io/file
                                                   (if-nil (:metadatafolder parsed-incremental-cli-options)
                                                           (rfs/parent
                                                            (:incrementaldataset parsed-incremental-cli-options)))
                                                   (str
                                                      (rfs/base-name
                                                         (:incrementaldataset parsed-incremental-cli-options)
                                                         true)
                                                      "_processed.csv")))
          :cli-dataset-name (:datasetname parsed-incremental-cli-options)
          :data-source (:datasource parsed-incremental-cli-options))

     (= mode "revertpredictions")
       (revert-predictions
          :predictions-dataset-file-path (:predictiondataset parsed-revert-predictions-cli-options)
          :metadata-folder-path (if-nil (:metadatafolder parsed-revert-predictions-cli-options)
                                        (rfs/parent
                                         (:predictiondataset parsed-revert-predictions-cli-options)))
          :predictions-product-file-path (if-nil (:predictionproduct parsed-revert-predictions-cli-options)

                                                 ; Just add _processed onto the end of the file base name if no product file name is given.
                                                 (clojure.java.io/file
                                                   (if-nil (:metadatafolder parsed-revert-predictions-cli-options)
                                                           (rfs/parent
                                                            (:predictiondataset parsed-revert-predictions-cli-options)))
                                                   (str
                                                      (rfs/base-name
                                                         (:predictiondataset parsed-revert-predictions-cli-options)
                                                         true)
                                                      "_reverted.csv")))
          :cli-dataset-name (:datasetname parsed-revert-predictions-cli-options))

    (= mode "revertsensitivity")
      (revert-sensitivity
         :sensitivity-dataset-file-path (:sensitivitydataset parsed-revert-sensitivity-cli-options)
         :metadata-folder-path (if-nil (:metadatafolder parsed-revert-sensitivity-cli-options)
                                        (rfs/parent
                                         (:sensitivitydataset parsed-revert-sensitivity-cli-options)))
         :sensitivity-product-file-path (if-nil (:sensitivityproduct parsed-revert-sensitivity-cli-options)

                                                 ; Just add _processed onto the end of the file base name if no product file name is given.
                                                 (clojure.java.io/file
                                                   (if-nil (:metadatafolder parsed-revert-sensitivity-cli-options)
                                                           (rfs/parent
                                                            (:sensitivitydataset parsed-revert-sensitivity-cli-options)))
                                                   (str
                                                      (rfs/base-name
                                                         (:sensitivitydataset parsed-revert-sensitivity-cli-options)
                                                         true)
                                                      "_reverted.csv")))
         :cli-dataset-name (:datasetname parsed-revert-sensitivity-cli-options)))

    ; Indicate end of program.
    (info "Finished.")
  )
)
