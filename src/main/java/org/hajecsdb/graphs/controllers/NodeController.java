package org.hajecsdb.graphs.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
class NodeController {

    @RequestMapping(method = RequestMethod.GET, path = "/Node")
    public String GetNode() {
        return "Node Controller";
    }
}
