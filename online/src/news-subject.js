const normal=value=>String(value||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
const subjects=[
  [/\bbitcoin\b|\bbtc\b/,'Bitcoin','BTC','coin'],[/\bethereum\b|\bether\b|\beth\b/,'Ethereum','ETH','coin'],
  [/\bsolana\b/,'Solana','SOL','coin'],[/\bxrp\b/,'XRP','XRP','coin'],[/\bnvidia\b|\bnvda\b/,'Nvidia','NVIDIA','chip'],
  [/\btesla\b|\btsla\b/,'Tesla','TESLA','car'],[/\banthropic\b/,'Anthropic','ANTHROPIC','chip'],[/\bopenai\b/,'OpenAI','OPENAI','chip'],
  [/\bs&p\s*500\b/,'S&P 500','S&P 500','market'],[/\bnasdaq\b/,'Nasdaq','NASDAQ','market'],[/\bdow jones\b/,'Dow Jones','DOW JONES','market'],
  [/\bfed\b|federal reserve|reserva federal/,'Reserva Federal','FED','bank'],[/\becb\b|\bbce\b|european central bank/,'Banco Central Europeo','BCE','bank'],
  [/\btrump\b/,'Donald Trump','TRUMP','politics'],[/\bvance\b/,'JD Vance','VANCE','politics'],
  [/\beurope\b|\beuropa\b|european union|union europea/,'Europa','EUROPA','politics'],[/\brussia\b|\brusia\b/,'Rusia','RUSIA','politics'],[/\bukraine\b|\bucrania\b/,'Ucrania','UCRANIA','politics'],
  [/\bbinance\b/,'Binance','BINANCE','exchange'],[/\bcoinbase\b/,'Coinbase','COINBASE','exchange'],[/\bthorchain\b/,'THORChain','THORCHAIN','exchange'],
  [/\bgnosis pay\b/,'Gnosis Pay','GNOSIS PAY','card'],[/\bhsbc\b/,'HSBC','HSBC','bank'],[/\bledger\b/,'Ledger','LEDGER','wallet'],
  [/\bamazon\b/,'Amazon','AMAZON','company'],[/\bapple\b/,'Apple','APPLE','company'],[/\bmicrosoft\b/,'Microsoft','MICROSOFT','chip'],
  [/\bgold\b|\boro\b/,'Oro','ORO','gold'],[/\boil\b|\bpetroleo\b/,'Petróleo','PETRÓLEO','energy']
];
export function newsSubjects(title){
  const text=normal(title);
  return subjects.map(([pattern,name,label,kind])=>({name,label,kind,index:text.search(pattern)})).filter(item=>item.index>=0).sort((a,b)=>a.index-b.index).slice(0,4);
}
export function headlineMovement(value){
  const text=normal(value).replace(/^[^\p{L}\p{N}]+/u,'').replace(/\s+/g,' ');
  const asset='(bitcoin|btc|ethereum|ether|eth|solana|sol|xrp|nvidia|nvda|tesla|tsla|nasdaq|s&p\\s*500|gold|oro|oil|petroleo)';
  const prefix=/^(?:the |el |la )?(?:falling|declining|sliding|rising|surging|caida|desplome|subida|alza)\s+(?:(?:del?\s+)?precio\s+de\s+|(?:price\s+)?of\s+|de\s+|in\s+)?/.exec(text);
  if(prefix){const found=new RegExp('^'+asset+'\\b').exec(text.slice(prefix[0].length));if(found)return {asset:found[1],direction:/rising|surging|subida|alza/.test(prefix[0])?'up':'down'};}
  const subject=new RegExp('^(?:(?:el|la|las|los|the) )?(?:(?:precio de|acciones de|shares of|price of) )?'+asset+"\\b(?:['’]s)?(?: (?:price|shares|stock|futures|cotizacion))?(?: (?:se|hoy|sharply|nearly|briefly|ligeramente)){0,3} ").exec(text);
  if(!subject)return null;
  const action=text.slice(subject[0].length);
  if(/^falls? short\b/.test(action))return null;
  if(/^(?:rally|rise|surge)\s+(?:fades|fizzles|stalls|falters|ends)\b|^sell[- ]?off\s+(?:fears|risk|warning)\b/.test(action))return null;
  if(/^drops?\s+(?:lawsuit|charges|case|plans|deal|acquisition|partnership|support|fees|difficulty)\b/.test(action))return null;
  if(/^(?:sell[- ]?off|rout|slump|crash(?:es|ing)?|slips?|fall(?:s|ing)?|drops?|tumbl(?:e|es|ing)|plung(?:e|es|ing)|sinks?|slid(?:e|es|ing)|retreats?|declin(?:e|es|ing)|dips?|loses?|cae|baja|retrocede|desploma|hunde|cede|pierde|caida|desplome)\b/.test(action))return {asset:subject[1],direction:'down'};
  if(/^(?:ris(?:e|es|ing)|gains?|jumps?|surges?|rall(?:y|ies)|climbs?|rebounds?|recovers?|sube|avanza|rebota|repunta|recupera|gana|subida|alza)\b/.test(action))return {asset:subject[1],direction:'up'};
  return null;
}
