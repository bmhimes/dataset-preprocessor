(ns adp.adp.core-test
  (:require [clojure.test :refer :all]
            [adp.adp.core :refer :all]
            [clojure.java.io]
            [criterium.core]))

(defn simple-dataset-csv
  [xlb xub f]
  "Creates a simple dataset CSV string of x,f(x) in domain [xlb,xub]."
  (reduce
   str
   (for [x (range xlb (+ xub 1))]
      (str x  "," (apply f [x]) (when (< x xub) "\r\n")))))

(deftest ^:unit testing-functions
  (testing "Testing simple dataset CSV functions."
    (let
      [simple-data (simple-dataset-csv 1.0 10.0 (fn [x] (* x x)))
       simple-data-correct-result "1.0,1.0\r\n2.0,4.0\r\n3.0,9.0\r\n4.0,16.0\r\n5.0,25.0\r\n6.0,36.0\r\n7.0,49.0\r\n8.0,64.0\r\n9.0,81.0\r\n10.0,100.0"
       simple-data-file-path "test/adp/adp/simple-data.csv"]
      (is (= simple-data simple-data-correct-result) "Testing simple-data correctness.")
      (spit simple-data-file-path simple-data)
      (is (= (.exists (clojure.java.io/as-file simple-data-file-path)) true) "Testing writing of CSV files.")
      (is (= (slurp simple-data-file-path) simple-data-correct-result) "Testing correctness of CSV file writing.")
      (clojure.java.io/delete-file simple-data-file-path)
      (is (= (.exists (clojure.java.io/as-file simple-data-file-path)) false) "Testing deletion of CSV files.")
      (is (= (clojure-csv.core/parse-csv simple-data) [["1.0" "1.0"] ["2.0" "4.0"] ["3.0" "9.0"] ["4.0" "16.0"] ["5.0" "25.0"] ["6.0" "36.0"] ["7.0" "49.0"] ["8.0" "64.0"] ["9.0" "81.0"] ["10.0" "100.0"]]) "Testing parsing of CSV string.")
  )))

(deftest ^:unit command-line-arguments
  (testing "Command-line interpretation."
    ; Define the testing command-line arguments.
    (let [initial-testing-args ["--mode" "initial" "--initialdataset" "\"dataset.csv\"" "--metadatafolder" "\"metadata\"" "--initialproduct" "\"processed.csv\""]
          initial-parsed-cli (:options
                                (clojure.tools.cli/parse-opts
                                 initial-testing-args
                                 startup-cli-options))]
    (testing "Initial command-line arguments."
      (is (= (:mode initial-parsed-cli) "initial") "Testing that mode option is parsed correctly.")))
    (let [claims-initial-args ["--initialdataset" "test/adp/adp/grahamcca/claims_training.csv" "--mode" "initial" "--metadatafolder" "test/adp/adp/grahamcca" "--outputs" "Total Incurred +6 Months" "--categories" "\"PolicyYear,Cause Type,Hazard Type,Body Part,Status,Gender,Marital Status,Coverage,Loss Time Range,Day of Week,Tenure,State - Benefits,Close Time Amount,State - Risk (Coverage),State - Risk (Occurrence),Injury Type,Loss Level Amt,Litigation,Loss Date Year,Loss Date Month,Loss Date Weekday,Loss Date Quarter,Tenure\"" "--datasource" "qv"]
          claims-startup-opts (:options
                                (clojure.tools.cli/parse-opts
                                 claims-initial-args
                                 startup-cli-options))
          claims-initial-opts (:options
                                (clojure.tools.cli/parse-opts
                                 claims-initial-args
                                 initial-mode-cli-options))]
      (testing "Claims initial arguments."
        (is (= (:mode claims-startup-opts) "initial") "Claims mode is correct.")
        (is (= (:outputs claims-initial-opts) ["Total Incurred +6 Months"]) "Outputs option correctly parsed.")
        (is (= (:categories claims-initial-opts) (vec (sort (set '("PolicyYear" "Cause Type" "Hazard Type" "Body Part" "Status" "Gender" "Marital Status" "Coverage" "Loss Time Range" "Day of Week" "Tenure" "State - Benefits" "Close Time Amount" "State - Risk (Coverage)" "State - Risk (Occurrence)" "Injury Type" "Loss Level Amt" "Litigation" "Loss Date Year" "Loss Date Month" "Loss Date Weekday" "Loss Date Quarter" "Tenure"))))) "Categories option correctly parsed.")
        (is (= (:datasource claims-initial-opts) "qv")))))
    ;(testing "initial command-line arguments."
;      (def parsed-initial-cli
;        (:options
;          (clojure.tools.cli/parse-opts initial-testing-args initial-mode-cli-options)))
;      (is (= (str (:initialdataset parsed-initial-cli)) "dataset.csv") "Testing that initial dataset option is parsed correctly.")
;      (is (= (str (:metadatafolder parsed-initial-cli)) "metadata") "Testing that metadata folder option is parsed correctly.")
;      (is (= (str (:initialproduct parsed-initial-cli)) "processed.csv") "Testing that initial product option is parsed correctly.")
;      )

)

(deftest ^:unit fixing-dble-quotes-unit
  (testing "Testing removal of double quotes"
    (is (= (fix-dble-quotes "\"path with spaces\"") "path with spaces")))
  (testing "Testing that normal strings are unaffected."
    (is (= (fix-dble-quotes "no_spaces") "no_spaces"))))

(deftest ^:unit numeric-processing-unit
  (testing "Testing absolute value functions."
    (is (= (abs 1.0) 1.0) "Positive number is unchanged.")
    (is (= (abs -1.0) 1.0) "Negative number is made positive.")
    (is (= (abs 0.0) 0.0) "Zero is unaffected."))
  (testing "Testing finding largest absolute value."
    (is (= (max-abs-vec [1.0 2.0 3.0]) 3.0) "Normal maximum number returned.")
    (is (= (max-abs-vec [-1.0 -2.0 -3.0]) 3.0) "Lowest negative number returned.")
    (is (= (max-abs-vec [-1.0 -2.0 3.0]) 3.0) "Largest postitive value returned among mixed numbers..")
    (is (= (max-abs-vec [1.0 2.0 -3.0]) 3.0) "Largest negative value returned among mixed numbers.")
    (is (= (max-abs-vec [3.0 -3.0]) 3.0) "Equal absolute values do not cause an error."))
  (testing "Testing walk of finding largest absolute value"
    (let
      [test-data (clojure-csv.core/parse-csv
                   (simple-dataset-csv 1.0 10.0
                     (fn [x] (* x x))))]
      (is (= (clojure.walk/walk #(parse-double (nth % 0)) max-abs-vec test-data) 10.0) "Testing that max-abs-vec work correctly on first field.")
      (is (= (clojure.walk/walk #(parse-double (nth % 1)) max-abs-vec test-data) 100.0) "Testing that max-abs-vec work correctly on second field.")))
  (testing "Calculation of scaling factors."
    (let [numerics ["i1" "o1"]
          sources ["i1" "i2" "o1"]
          data [[1.0 "A" 1.0]
                [2.0 "B" 4.0]
                [3.0 "C" 9.0]]
          scaling-factors [["i1" "o1"]
                           [3.0 9.0]]
          calc-scaling-factors (calculate-scaling-factors
                                :numeric-field-names numerics
                                :source-field-names sources
                                :initial-observation-data data)]
      (is (= scaling-factors calc-scaling-factors) "Correct scaling factors calculated in normal situation."))
    (let [numerics ["i1"]
          sources ["i1"]
          data [[0.0]
                [0.0]
                [0.0]]
          scaling-factors [["i1"]
                           [1.0]]
          calc-scaling-factors (calculate-scaling-factors
                                :numeric-field-names numerics
                                :source-field-names sources
                                :initial-observation-data data)]
      (is (= scaling-factors calc-scaling-factors) "1 replaces 0 scaling factor."))
    ))

(deftest ^:unit processed-field-name-parsing
  (let [processed-field-names ["i1__A" "i1__B" "i2" "bias" "o1"]]
    (testing "Parsing processed input field names."
      (is (= ["i1__A" "i1__B" "i2" "bias"] (get-processed-input-field-names processed-field-names)) "Input field names correctly returned."))
    (testing "Parsing processed output field names."
      (is (= ["o1"] (get-processed-output-field-names processed-field-names)) "Output field names correctly returned."))))

(deftest ^:unit sensitivity-reversion
  (testing "Small simulation based on sq40."
    (let [sensitivity [[1.025]
                       [0.0]]
          reverted-sensitivity [[40.9999999999999]
                                [0.0]]
          processed-inputs ["i1"]
          processed-outputs ["o1"]
          sensitivity-product [["Processed Input" "o1 Sensitivity"]
                               ["i1" "40.9999999999999"]]]
      (is (= sensitivity-product (generate-reverted-sensitivity-table :reverted-sensitivity reverted-sensitivity :proc-inputs processed-inputs :proc-outputs processed-outputs))))))

(deftest ^:unit prediction-reversion
  (testing "Small simulation based on squaring input.")
    (let [reverted-predictions [[1.0]
                                [4.0]
                                [9.0]
                                [16.0]]
          processed-output-headers ["o1"]
          reverted-predictions-product [["Predicted o1"]
                                        ["1.0"]
                                        ["4.0"]
                                        ["9.0"]
                                        ["16.0"]]
          test-reverted-predictions-product (generate-reverted-predictions-table
                                             :reverted-predictions reverted-predictions
                                             :proc-outputs processed-output-headers)]
      (is (= test-reverted-predictions-product reverted-predictions-product) "Correct table generated.")))

(deftest ^:unit category-processing-unit
  (testing "Distinct values correctly determined."
    (let [categories ["i2"]
          sources ["i1" "i2" "o1"]
          data [[1.0 "A" 1.0]
                [2.0 "B" 4.0]
                [3.0 "A" 3.0]]
          distinct-vals [["i2"]
                         ['("A" "B")]]
          calc-distinct-vals (calculate-distinct-values
                              :categoric-field-names categories
                              :source-field-names sources
                              :initial-observation-data data)]
      (is (= distinct-vals calc-distinct-vals)))))

(deftest ^:quick-bench max-abs-vec-qb
  (do
    (println "Benchmarking performance of 1,000 items in a vector.")
    (let
      [test-vec (vec (range -500.0 500.0))]
      (criterium.core/quick-bench (max-abs-vec test-vec)))))

(deftest ^:scaling-bench max-abs-vec-sb
  (do
    (println "Benchmarking performance of 1,000,000 items in a vector.")
    (let
      [test-vec (vec (range -500000.0 500000.0))]
      (criterium.core/bench (max-abs-vec test-vec)))))

(deftest ^:simulation process-initial-data-sq-40-1-1
  (testing "Testing simple process on 40 rows of \"output equals input squared.\""
    (-main "--mode" "initial" "--initialdataset" "\"test/adp/adp/sq_40_1_1/sq_40_1_1.csv\"" "--metadatafolder" "\"test/adp/adp/sq_40_1_1\"" "--initialproduct" "\"test/adp/adp/sq_40_1_1/sq_40_1_1_processed.csv\"")))

(deftest ^:simulation process-incremental-data-sq-80-1-1
  (testing "Testing simple incremental process on 80 rows of \"output equals input squared.\""
    (-main "--mode" "incremental" "--incrementaldataset" "\"test/adp/adp/sq_40_1_1/sq_80_1_1.csv\"" "--metadatafolder" "\"test/adp/adp/sq_40_1_1\"")))

(deftest ^:simulation process-initial-data-simpleiif-20-2-1
  (testing "Testing simple process on 20 rows of \"square if A, or square-root if B\""
    (-main "--mode" "initial" "--initialdataset" "\"test/adp/adp/simpleif_20_2_1.csv\"" "--metadatafolder" "\"test/adp/adp\"" "--initialproduct" "\"test/adp/adp/simpleif_20_2_1_processed.csv\"" "--categories" "i1")))

(deftest ^:simulation revert-predictions-data-sq-80-1-1
  (testing "Testing simple prediction reversion on 80 rows of \"output equals input squared.\""
    (-main "--mode" "revertpredictions" "--predictiondataset" "\"test/adp/adp/sq_40_1_1/sq_80_1_1_processed_predictions.csv\"" "--metadatafolder" "\"test/adp/adp/sq_40_1_1\"")))

(deftest ^:simulation revert-sensitivity-data-sq-40-1-1
  (testing "Testing simple senstivity reversion on 40 rows of \"output equals input squared.\""
    (-main "--mode" "revertsensitivity" "--sensitivitydataset" "\"test/adp/adp/sq_40_1_1/sq_40_1_1_processed_sensitivity.csv\"" "--metadatafolder" "\"test/adp/adp/sq_40_1_1\"")))

(deftest ^:claims process-claims
  (testing "Testing initial process on claims."
    (-main "--mode" "initial" "--initialdataset" "test/adp/adp/grahamcca/claims_losses_training.csv" "--metadatafolder" "test/adp/adp/grahamcca" "--outputs" "Total Incurred +6 Months" "--categories" "\"PolicyYear,Cause Type,Hazard Type,Body Part,Status,Gender,Marital Status,Coverage,Loss Time Range,Day of Week,Tenure,State - Benefits,Close Time Amount,State - Risk (Occurrence),Injury Type,Loss Level Amt,Litigation,Loss Date Year,Loss Date Month,Loss Date Weekday,Loss Date Quarter,Tenure\"" "--datasource" "qv")
    (-main "--mode" "incremental" "--incrementaldataset" "test/adp/adp/grahamcca/claims_losses_validation.csv" "--metadatafolder" "test/adp/adp/grahamcca" "--datasource" "qv")))

(deftest ^:quick-bench process-initial-data-noise96-100000-99-1
  (testing "Testing simple process on 40 rows of \"output equals input squared.\""
    (-main "--mode" "initial" "--initialdataset" "\"test/adp/adp/noise96_100000_99_1.csv\"" "--metadatafolder" "\"test/adp/adp\"")))

(deftest ^:scaling-bench process-initial-data-noise96-100000-99-1-qb
  (testing "Testing simple process on 40 rows of \"output equals input squared.\""
    (criterium.core/quick-bench (-main "--mode" "initial" "--initialdataset" "\"test/adp/adp/noise96_100000_99_1.csv\"" "--metadatafolder" "\"test/adp/adp\""))))
