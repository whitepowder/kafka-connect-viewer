
global.localStorage={getItem:()=>null,setItem:()=>{}};
global.navigator={language:"ru-RU"};
global.location={hash:"",addEventListener:()=>{}};
global.window={addEventListener:()=>{}};
global.document={
  documentElement:{lang:"",dataset:{}},
  getElementById:(id)=>({addEventListener:()=>{},append:()=>{},replaceChildren:()=>{},classList:{toggle:()=>{}},hidden:false}),
  addEventListener:()=>{},
  createElement:()=>({setAttribute:()=>{},append:()=>{},classList:{add:()=>{},toggle:()=>{}},dataset:{}}),
  createTextNode:(text)=>({nodeType:3,textContent:String(text)}),
  querySelectorAll:()=>[],
  querySelector:()=>null,
  activeElement:null
};
global.fetch=async()=>({ok:true,text:async()=>JSON.stringify({clusters:[]}),statusText:"OK"});
require(require("path").resolve(process.argv[2] || "static/app.js"));
setTimeout(()=>process.exit(0),20);
