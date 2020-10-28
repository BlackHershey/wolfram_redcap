function reformatDxFieldLabel(dxCodeName) {
	var divName = dxCodeName + "_dx_label";
	// var labelDivHTML = "<h6>" + dxName + ": diagnosis</h6><br />";
	var labelDivHTML = "";
	var labelDiv = document.getElementById(divName);
	var dxDateRule = labelDiv.getElementsByClassName("dx_date_rule")[0].childNodes[0].innerHTML;
	var dxAge = Number(labelDiv.getElementsByClassName("dx_age")[0].childNodes[0].innerHTML);
	var dxDateNotes = labelDiv.getElementsByClassName("dx_date_notes")[0].childNodes[0].innerHTML;
	var dxDate = labelDiv.getElementsByClassName("dx_date")[0].childNodes[0].innerHTML;
	var dxDateEst = labelDiv.getElementsByClassName("dx_date_est")[0].childNodes[0].innerHTML;

	// parse estimated dx date
	if ( dxDateEst.length > 6 ){
		var dxDateEstArray = dxDateEst.split("-");
		var dxYear = dxDateEstArray[0];
		var dxMonth = Number(dxDateEstArray[1]);
		dxMonth = dxMonth - 1;
		var dxDay = dxDateEstArray[2];
		var dxDateObj = new Date(dxYear, dxMonth, dxDay);
		dxDay = parseInt(dxDay).toString();
		var dxMonthLong = dxDateObj.toLocaleString('default', { month: 'long' });
	} else if ( dxDate.length > 6 ){
		var dxDateArray = dxDate.split("-");
		var dxYear = dxDateArray[0];
		var dxMonth = Number(dxDateArray[1]);
		dxMonth = dxMonth - 1;
		var dxDay = dxDateArray[2];
		var dxDateObj = new Date(dxYear, dxMonth, dxDay);
		dxDay = parseInt(dxDay).toString();
		var dxMonthLong = dxDateObj.toLocaleString('default', { month: 'long' });
	}

	// dx date/age
	labelDivHTML += "<span style=\"color:#0e1f8a;\">Diagnosis:</span> "
	// test dx date rule
    switch(dxDateRule) {
    	case "Date":
    		labelDivHTML += "The <i>actual dx date</i> is " + dxMonthLong + " " + dxDay + ", " + dxYear + "; age = " + dxAge.toFixed(2) + ".";
            break;
  		case "Age":
    		labelDivHTML += "<i>Estimated age</i> at dx = " + dxAge.toFixed(1);
			break;
		case "Month":
			labelDivHTML += "<i>Estimated</i> dx at <i>" + dxMonthLong + " " + dxYear + "</i>; age = " + dxAge.toFixed(2);
			break;
		case "Year":
			labelDivHTML += "<i>Estimated</i> dx in <i>year " + dxYear + "</i> (used " + dxMonthLong + " " + dxDay + ", " + dxYear + "); age = " + dxAge.toFixed(2);
			break;
		case "Clinic dx":
			labelDivHTML += "<i>Clinic dx</i> at the " + dxYear + " clinic (" + dxMonthLong + " " + dxYear + "); age = " + dxAge.toFixed(2);
			break;
		case "Dx age unknown":
			labelDivHTML += "Diagnosis present, but age unknown";
			break;
		case "No dx":
			labelDivHTML += "No diagnosis";
			break;
  		default:
    		// code block
	}
	labelDivHTML += "<br /><br />";

	// dx date/age notes
	labelDivHTML += "<span style=\"color:#0e1f8a;\">Notes:</span> "
	labelDivHTML += dxDateNotes;
    labelDiv.innerHTML = labelDivHTML;
}
window.addEventListener('load', function(){reformatDxFieldLabel("wfs")})
window.addEventListener('load', function(){reformatDxFieldLabel("dm")})
window.addEventListener('load', function(){reformatDxFieldLabel("di")})
window.addEventListener('load', function(){reformatDxFieldLabel("hearloss")})
window.addEventListener('load', function(){reformatDxFieldLabel("oa")})
window.addEventListener('load', function(){reformatDxFieldLabel("bladder")})