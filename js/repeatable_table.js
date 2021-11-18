function reformatRepeatableDivs() {
    // find repeatable div
    var repeatableDiv = document.getElementById("repeating_forms_table_parent");
    var repeatableTables = repeatableDiv.getElementsByTagName("TABLE");

    var outputDiv = "";

    for (table of repeatableTables){
        // get form name
        var formNameDiv = table.getElementsByClassName("float-left")[0];
        var formName = formNameDiv.childNodes[0].nodeValue.trim();
        console.log("Form name = " + formName);
        var tableRows = table.getElementsByTagName("TR");
        //console.log("Num TRs = " + tableRows.length);

        var i;
        var rowText = "";
        for (i=1; i < tableRows.length-1; i++){
            console.log(tableRows[i]);
            rowText = tableRows[i].getElementsByTagName("TD")[2].innerHTML;
            // outputDiv += rowText + "<br />";
        }
    }
    
    // repeatableDiv.innerHTML = outputDiv;

    // Bootstrap uses columns that are 12 units long, so to make a div take up a whole column,
    // set class="col-12"
}
window.addEventListener('load', function(){reformatRepeatableDivs()})