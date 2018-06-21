<?php
/**
 * ARC2 RDF Store INSERT Query Handler.
 *
 * @author Benjamin Nowack <bnowack@semsol.com>
 * @license W3C Software License and GPL
 * @homepage <https://github.com/semsol/arc2>
 */
ARC2::inc('StoreQueryHandler');

class ARC2_StoreInsertQueryHandler extends ARC2_StoreQueryHandler
{
    public function __construct($a, &$caller)
    {/* caller has to be a store */
        parent::__construct($a, $caller);
    }

    public function __init()
    {
        parent::__init();
        $this->store = $this->caller;
    }

    protected function getTablePrefix()
    {
        $prefix = $this->v('db_table_prefix', '', $this->a);
        $prefix .= $prefix ? '_' : '';

        $store = $this->v('store_name', 'arc', $this->a);
        $store .= $store ? '_' : '';

        return $prefix . $store;
    }

    public function runQuery($infos, $keep_bnode_ids = 0)
    {
        $this->infos = $infos;
        /*
         * INSERT INTO, without a WHERE clause
         */
        if (!$this->v('pattern', [], $this->infos['query'])) {
            $triples = $this->infos['query']['construct_triples'];
            /* don't execute empty INSERTs as they trigger a LOAD on the graph URI */
            if (\is_array($triples)) {
                // remove entries, which are already in the database
                foreach ($triples as $key => $entry) {
                    /*
                        an entry looks like:

                        array(9) {
                            ["type"]=>
                            string(6) "triple"
                            ["s"]=>
                            string(5) "urn:1"
                            ["p"]=>
                            string(5) "urn:2"
                            ["o"]=>
                            string(5) "urn:3"
                            ["s_type"]=>
                            string(3) "uri"
                            ["p_type"]=>
                            string(3) "uri"
                            ["o_type"]=>
                            string(3) "uri"
                            ["o_datatype"]=>
                            string(0) ""
                            ["o_lang"]=>
                            string(0) ""
                        }
                     */
                    $row = $this->store->getDBObject()->fetchRow('
                        SELECT *
                          FROM '.$this->getTablePrefix().'triple t
                               LEFT JOIN '.$this->getTablePrefix().'s2val sval ON t.s = sval.id
                               LEFT JOIN '.$this->getTablePrefix().'id2val pval ON t.p = pval.id
                               LEFT JOIN '.$this->getTablePrefix().'o2val oval ON t.o = oval.id
                               LEFT JOIN '.$this->getTablePrefix().'g2t gt ON t.t = gt.t
                               LEFT JOIN '.$this->getTablePrefix().'id2val gval ON gt.g = gval.id
                         WHERE sval.val = "'.$entry['s'].'"
                               AND pval.val = "'.$entry['p'].'"
                               AND oval.val = "'.$entry['o'].'"
                               AND gval.val = "'.$infos['query']['target_graph'].'"
                         LIMIT 1
                    ');
                    // if row is set, it means we already have this triple in our store, so ignore it
                    if (\is_array($row) && 0 < \count($row)) {
                        unset($triples[$key]);
                    }
                }

                if (0 < \count($triples)) {
                    return $this->store->insert($triples, $this->infos['query']['target_graph'], $keep_bnode_ids);
                }
            } else {
                return ['t_count' => 0, 'load_time' => 0];
            }
        /*
         * INSERT INTO WHERE
         */
        } else {
            $keep_bnode_ids = 1;
            ARC2::inc('StoreConstructQueryHandler');
            $h = new ARC2_StoreConstructQueryHandler($this->a, $this->store);
            $sub_r = $h->runQuery($this->infos);
            if ($sub_r) {
                return $this->store->insert($sub_r, $this->infos['query']['target_graph'], $keep_bnode_ids);
            }

            return ['t_count' => 0, 'load_time' => 0];
        }
    }
}
