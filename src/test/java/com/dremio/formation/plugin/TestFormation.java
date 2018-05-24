package com.dremio.formation.plugin;

import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.source.proto.SourceConfig;

public class TestFormation extends BaseTestQuery {

  @Test
  public void test1() throws Exception {
    SourceConfig c = new SourceConfig();
    FormationConfig conf = new FormationConfig();
    c.setConnectionConf(conf);
    c.setName("flight");
    c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    CatalogServiceImpl cserv = (CatalogServiceImpl) getBindingProvider().lookup(CatalogService.class);
    cserv.createSourceIfMissingWithThrow(c);
    test("create table flight.blue as select * from sys.options");
//    test("create table flight.lineitem as select * from dfs_root.opt.data.sf1p.lineitem");
//    test("select count(*) from flight.lineitem");
    test("select * from flight.blue");
    test("select * from flight.blue");
  }
}
